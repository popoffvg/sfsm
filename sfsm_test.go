package sfsm

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testState string

const (
	stateInit    testState = "init"
	stateRunning testState = "running"
	stateDone    testState = "done"
	stateFailed  testState = "failed"
)

type testEvent struct {
	action string
}

func TestSFSM_BasicTransition(t *testing.T) {
	fsm := NewSFSM(stateInit, func(current testState, event testEvent) (testState, error) {
		switch current { //nolint:exhaustive,lll // terminal states (done/failed) intentionally fall through to invalid-transition error
		case stateInit:
			if event.action == "start" {
				return stateRunning, nil
			}
		case stateRunning:
			if event.action == "complete" {
				return stateDone, nil
			}
		}
		return current, fmt.Errorf("invalid transition from %s with action %s", current, event.action)
	}, nil)
	defer func() { _ = fsm.Close() }()

	ctx := context.Background()
	require.Equal(t, stateInit, fsm.Get())

	err := fsm.Apply(ctx, testEvent{action: "start"})
	require.NoError(t, err)
	require.Equal(t, stateRunning, fsm.Get())

	err = fsm.Apply(ctx, testEvent{action: "complete"})
	require.NoError(t, err)
	require.Equal(t, stateDone, fsm.Get())
}

func TestSFSM_FailedTransition(t *testing.T) {
	fsm := NewSFSM(stateInit, func(current testState, event testEvent) (testState, error) {
		if event.action == "start" {
			return stateRunning, nil
		}
		return current, fmt.Errorf("unexpected action: %s", event.action)
	}, nil)
	defer func() { _ = fsm.Close() }()

	ctx := context.Background()
	require.Equal(t, stateInit, fsm.Get())

	err := fsm.Apply(ctx, testEvent{action: "invalid"})
	require.Error(t, err)
	require.Equal(t, stateInit, fsm.Get())

	err = fsm.Apply(ctx, testEvent{action: "start"})
	require.NoError(t, err)
	require.Equal(t, stateRunning, fsm.Get())
}

func TestSFSM_ConcurrentApply(t *testing.T) {
	var processCount atomic.Int32
	fsm := NewSFSM(int32(0), func(current int32, event int32) (int32, error) {
		processCount.Add(1)
		time.Sleep(10 * time.Millisecond)
		return current + event, nil
	}, nil)
	defer func() { _ = fsm.Close() }()

	ctx := context.Background()
	const numGoroutines = 10
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for range numGoroutines {
		go func() {
			defer wg.Done()
			err := fsm.Apply(ctx, 1)
			assert.NoError(t, err)
		}()
	}

	wg.Wait()

	require.Equal(t, int32(numGoroutines), fsm.Get())           //nolint:lll // G115: small test values, never overflow int32
	require.Equal(t, int32(numGoroutines), processCount.Load()) //nolint:lll // G115: small test values, never overflow int32
}

func TestSFSM_ConcurrentReads(t *testing.T) {
	fsm := NewSFSM(int32(0), func(current int32, event int32) (int32, error) {
		time.Sleep(50 * time.Millisecond)
		return event, nil
	}, nil)
	defer func() { _ = fsm.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	for i := range 10 {
		wg.Go(func() {
			err := fsm.Apply(ctx, int32(i))
			require.NoError(t, err)
		})
		wg.Go(func() {
			err := fsm.Wait(ctx, func(prev, current int32) bool {
				return current >= int32(i)
			})
			require.NoError(t, err)
		})
	}

	for range 10 {
		wg.Go(func() {
			fsm.Get()
			time.Sleep(10 * time.Millisecond)
		})
	}

	wg.Wait()
	require.GreaterOrEqual(t, fsm.Get(), int32(0))
	require.Less(t, fsm.Get(), int32(10))
}

func TestSFSM_OrderedProcessing(t *testing.T) {
	var processedEvents []string
	var mu sync.Mutex

	fsm := NewSFSM(stateInit, func(current testState, event testEvent) (testState, error) {
		mu.Lock()
		processedEvents = append(processedEvents, event.action)
		mu.Unlock()
		return current, nil
	}, nil)
	defer func() { _ = fsm.Close() }()

	ctx := context.Background()
	events := []string{"event1", "event2", "event3", "event4", "event5"}
	for _, action := range events {
		err := fsm.Apply(ctx, testEvent{action: action})
		require.NoError(t, err)
	}

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, events, processedEvents)
}

func TestSFSM_Close(t *testing.T) {
	fsm := NewSFSM(stateInit, func(current testState, event testEvent) (testState, error) {
		return current, nil
	}, nil)

	err := fsm.Close()
	require.NoError(t, err)

	select {
	case <-fsm.doneCh:
	case <-time.After(1 * time.Second):
		t.Fatal("FSM did not close within timeout")
	}
}

func TestSFSM_StateNotMutatedOnError(t *testing.T) {
	callCount := 0
	fsm := NewSFSM(stateInit, func(current testState, event testEvent) (testState, error) {
		callCount++
		if event.action == "fail" {
			return stateFailed, errors.New("intentional failure")
		}
		if event.action == "start" {
			return stateRunning, nil
		}
		return current, nil
	}, nil)
	defer func() { _ = fsm.Close() }()

	ctx := context.Background()
	err := fsm.Apply(ctx, testEvent{action: "start"})
	require.NoError(t, err)
	require.Equal(t, stateRunning, fsm.Get())

	err = fsm.Apply(ctx, testEvent{action: "fail"})
	require.Error(t, err)
	require.Equal(t, stateRunning, fsm.Get())

	require.Equal(t, 2, callCount)
}

func TestSFSM_PrevStateCapture(t *testing.T) {
	var capturedPrevStates []testState
	var mu sync.Mutex

	fsm := NewSFSM(stateInit, func(current testState, event testEvent) (testState, error) {
		mu.Lock()
		capturedPrevStates = append(capturedPrevStates, current)
		mu.Unlock()

		switch event.action {
		case "start":
			return stateRunning, nil
		case "complete":
			return stateDone, nil
		case "fail":
			return stateFailed, nil
		}
		return current, nil
	}, nil)
	defer func() { _ = fsm.Close() }()

	ctx := context.Background()
	err := fsm.Apply(ctx, testEvent{action: "start"})
	require.NoError(t, err)

	err = fsm.Apply(ctx, testEvent{action: "complete"})
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []testState{stateInit, stateRunning}, capturedPrevStates)
}

func TestSFSM_MultipleTransitions(t *testing.T) {
	fsm := NewSFSM(stateInit, func(current testState, event testEvent) (testState, error) {
		switch current { //nolint:exhaustive,lll // terminal states (done/failed) intentionally fall through to invalid-transition error
		case stateInit:
			if event.action == "start" {
				return stateRunning, nil
			}
		case stateRunning:
			if event.action == "complete" {
				return stateDone, nil
			}
			if event.action == "fail" {
				return stateFailed, nil
			}
		}
		return current, errors.New("invalid transition")
	}, nil)
	defer func() { _ = fsm.Close() }()

	ctx := context.Background()
	transitions := []struct {
		event         testEvent
		expectedState testState
		shouldError   bool
	}{
		{testEvent{action: "start"}, stateRunning, false},
		{testEvent{action: "invalid"}, stateRunning, true},
		{testEvent{action: "complete"}, stateDone, false},
		{testEvent{action: "start"}, stateDone, true},
	}

	for _, tc := range transitions {
		err := fsm.Apply(ctx, tc.event)
		if tc.shouldError {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, tc.expectedState, fsm.Get())
	}
}

func TestSFSM_Wait(t *testing.T) {
	fsm := NewSFSM(stateInit, func(current testState, event testEvent) (testState, error) {
		switch current { //nolint:exhaustive,lll // terminal states (done/failed) intentionally fall through to invalid-transition error
		case stateInit:
			if event.action == "start" {
				return stateRunning, nil
			}
		case stateRunning:
			if event.action == "complete" {
				return stateDone, nil
			}
		}
		return current, errors.New("invalid transition")
	}, nil)
	defer func() { _ = fsm.Close() }()

	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Go(func() {
		time.Sleep(50 * time.Millisecond)
		err := fsm.Apply(ctx, testEvent{action: "start"})
		require.NoError(t, err)
		time.Sleep(50 * time.Millisecond)
		err = fsm.Apply(ctx, testEvent{action: "complete"})
		require.NoError(t, err)
	})

	err := fsm.Wait(ctx, Equal(stateDone))
	require.NoError(t, err)
	require.Equal(t, stateDone, fsm.Get())

	wg.Wait()
}

func TestSFSM_WaitAlreadySatisfied(t *testing.T) {
	fsm := NewSFSM(stateRunning, func(current testState, event testEvent) (testState, error) {
		return current, nil
	}, nil)
	defer func() { _ = fsm.Close() }()

	ctx := context.Background()
	err := fsm.Wait(ctx, Equal(stateRunning))
	require.NoError(t, err)
}

func TestSFSM_WaitTimeout(t *testing.T) {
	fsm := NewSFSM(stateInit, func(current testState, event testEvent) (testState, error) {
		return current, nil
	}, nil)
	defer func() { _ = fsm.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := fsm.Wait(ctx, Equal(stateDone))
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestSFSM_WaitCustomPredicate(t *testing.T) {
	fsm := NewSFSM(stateInit, func(current testState, event testEvent) (testState, error) {
		if event.action == "start" {
			return stateRunning, nil
		}
		if event.action == "complete" {
			return stateDone, nil
		}
		if event.action == "fail" {
			return stateFailed, nil
		}
		return current, errors.New("invalid action")
	}, nil)
	defer func() { _ = fsm.Close() }()

	ctx := context.Background()

	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = fsm.Apply(ctx, testEvent{action: "start"})
		time.Sleep(50 * time.Millisecond)
		_ = fsm.Apply(ctx, testEvent{action: "complete"})
	}()

	isFinal := func(_, current testState) bool {
		return current == stateDone || current == stateFailed
	}

	err := fsm.Wait(ctx, isFinal)
	require.NoError(t, err)
	require.Equal(t, stateDone, fsm.Get())
}

func TestSFSM_WaitAbortsOnClose(t *testing.T) {
	fsm := NewSFSM(stateInit, func(current testState, event testEvent) (testState, error) {
		return current, nil
	}, nil)

	var waitErr error
	wg := sync.WaitGroup{}
	wg.Go(func() {
		waitErr = fsm.Wait(t.Context(), Equal(stateDone))
	})

	_ = fsm.Close()
	wg.Wait()

	require.ErrorIs(t, waitErr, ErrFSMClosed)
}

func TestSFSM_ApplyContextCanceled(t *testing.T) {
	fsm := NewSFSM(stateInit, func(current testState, event testEvent) (testState, error) {
		time.Sleep(200 * time.Millisecond)
		return stateRunning, nil
	}, nil)
	defer func() { _ = fsm.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := fsm.Apply(ctx, testEvent{action: "start"})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, stateInit, fsm.Get())
}

func TestSFSM_ApplyContextTimeout(t *testing.T) {
	fsm := NewSFSM(stateInit, func(current testState, event testEvent) (testState, error) {
		time.Sleep(200 * time.Millisecond)
		return stateRunning, nil
	}, nil)
	defer func() { _ = fsm.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := fsm.Apply(ctx, testEvent{action: "start"})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, stateInit, fsm.Get())
}

func TestSFSM_ApplyContextCanceledWhileWaiting(t *testing.T) {
	fsm := NewSFSM(stateInit, func(current testState, event testEvent) (testState, error) {
		time.Sleep(100 * time.Millisecond)
		return stateRunning, nil
	}, nil)
	defer func() { _ = fsm.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	err := fsm.Apply(ctx, testEvent{action: "start"})
	require.NoError(t, err)
	require.Equal(t, stateRunning, fsm.Get())

	ctx2, cancel2 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel2()

	err = fsm.Apply(ctx2, testEvent{action: "complete"})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, stateRunning, fsm.Get())
}
