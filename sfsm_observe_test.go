package sfsm

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avast/retry-go"
	"github.com/stretchr/testify/require"
)

// retryLibraryDefaultAttempts is retry-go's own Attempts default, which a policy that
// names no attempt count would otherwise inherit.
const retryLibraryDefaultAttempts = 10

func observeEvery(pace time.Duration, attempts uint) []RetryOption {
	return []RetryOption{
		retry.Attempts(attempts),
		retry.Delay(pace),
		retry.DelayType(retry.FixedDelay),
	}
}

func observeForever(pace time.Duration) []RetryOption {
	return []RetryOption{
		retry.Delay(pace),
		retry.DelayType(retry.FixedDelay),
	}
}

func newCountingSFSM(t *testing.T) *SFSM[int32, int32] {
	t.Helper()

	fsm := NewSFSM(int32(0), func(_ int32, event int32) (int32, error) {
		return event, nil
	}, nil)
	t.Cleanup(func() { _ = fsm.Close() })

	return fsm
}

func TestObserveExternalState_StopsOnContextCancel(t *testing.T) {
	fsm := newCountingSFSM(t)

	ctx, cancel := context.WithCancel(t.Context())

	var observations atomic.Int32
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ObserveExternalState(
			ctx,
			fsm,
			func(context.Context) int32 { return observations.Add(1) },
			observeForever(time.Millisecond)...,
		)
	}()

	require.Eventually(t, func() bool {
		return observations.Load() > 2
	}, time.Second, time.Millisecond, "the poll must keep observing until it is told to stop")

	cancel()

	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("the poll did not return after the context was cancelled")
	}

	settled := observations.Load()
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, settled, observations.Load(), "the poll must not observe after it returned")
}

func TestObserveExternalState_StopsWhenTheMachineIsClosed(t *testing.T) {
	fsm := NewSFSM(int32(0), func(_ int32, event int32) (int32, error) {
		return event, nil
	}, nil)

	var observations atomic.Int32
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ObserveExternalState(
			t.Context(),
			fsm,
			func(context.Context) int32 { return observations.Add(1) },
			observeForever(time.Millisecond)...,
		)
	}()

	require.Eventually(t, func() bool {
		return observations.Load() > 2
	}, time.Second, time.Millisecond, "the poll must keep observing until it is told to stop")

	require.NoError(t, fsm.Close())

	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("the poll did not return after the machine was closed")
	}
}

func TestObserveExternalState_PacesWithThePolicyItWasGiven(t *testing.T) {
	fsm := newCountingSFSM(t)

	const pace = 40 * time.Millisecond
	const observationsToTime = 4

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	timed := make(chan struct{})
	var observations atomic.Int32
	started := time.Now()
	go ObserveExternalState(
		ctx,
		fsm,
		func(context.Context) int32 {
			count := observations.Add(1)
			if count == observationsToTime {
				close(timed)
			}
			return count
		},
		observeForever(pace)...,
	)

	select {
	case <-timed:
	case <-time.After(5 * time.Second):
		t.Fatal("the poll did not reach the observation being timed")
	}

	elapsed := time.Since(started)
	require.GreaterOrEqual(
		t,
		elapsed,
		(observationsToTime-1)*pace,
		"the poll must wait the policy delay between one observation and the next",
	)
}

func TestObserveExternalState_LeavesTheObserverToReportItsOwnFailure(t *testing.T) {
	fsm := NewSFSM(stateInit, func(_ testState, event testEvent) (testState, error) {
		if event.action == "unreachable" {
			return stateFailed, nil
		}
		return stateRunning, nil
	}, nil)
	t.Cleanup(func() { _ = fsm.Close() })

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	go ObserveExternalState(
		ctx,
		fsm,
		func(context.Context) testEvent { return testEvent{action: "unreachable"} },
		observeForever(time.Millisecond)...,
	)

	require.NoError(t, fsm.Wait(ctx, Equal(stateFailed)))
}

func TestObserveExternalState_KeepsGoingWhenTheMachineRejectsTheEvent(t *testing.T) {
	fsm := NewSFSM(stateInit, func(current testState, _ testEvent) (testState, error) {
		return current, errors.New("sfsm test: the machine rejects every event")
	}, nil)
	t.Cleanup(func() { _ = fsm.Close() })

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var observations atomic.Int32
	go ObserveExternalState(
		ctx,
		fsm,
		func(context.Context) testEvent {
			observations.Add(1)
			return testEvent{action: "rejected"}
		},
		observeForever(time.Millisecond)...,
	)

	require.Eventually(
		t,
		func() bool { return observations.Load() > 3 },
		time.Second,
		time.Millisecond,
		"an Apply error that is not ErrFSMClosed must not end the observation",
	)
}

func TestObserveExternalState_DoesNotObserveOnACancelledContext(t *testing.T) {
	fsm := newCountingSFSM(t)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	var observations atomic.Int32
	ObserveExternalState(
		ctx,
		fsm,
		func(context.Context) int32 { return observations.Add(1) },
		observeForever(time.Millisecond)...,
	)

	require.Zero(t, observations.Load(), "a cancelled context must not buy one last observation")
}

func TestObserveExternalState_DoesNotObserveOnAClosedMachine(t *testing.T) {
	fsm := NewSFSM(int32(0), func(_ int32, event int32) (int32, error) {
		return event, nil
	}, nil)
	require.NoError(t, fsm.Close())

	var observations atomic.Int32
	ObserveExternalState(
		t.Context(),
		fsm,
		func(context.Context) int32 { return observations.Add(1) },
		observeForever(time.Millisecond)...,
	)

	require.Zero(t, observations.Load(), "a closed machine must not buy one last observation")
}

func TestObserveExternalState_StopsWhenThePolicyRunsOutOfAttempts(t *testing.T) {
	fsm := newCountingSFSM(t)

	const attempts = 3

	var observations atomic.Int32
	ObserveExternalState(
		t.Context(),
		fsm,
		func(context.Context) int32 { return observations.Add(1) },
		observeEvery(time.Millisecond, attempts)...,
	)

	require.Equal(
		t,
		int32(attempts),
		observations.Load(),
		"an explicit attempt count is the only thing that ends a healthy observation early",
	)
}

func TestObserveExternalState_OutlivesTheRetryLibraryDefaultAttempts(t *testing.T) {
	fsm := newCountingSFSM(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var observations atomic.Int32
	go ObserveExternalState(
		ctx,
		fsm,
		func(context.Context) int32 { return observations.Add(1) },
	)

	require.Eventually(
		t,
		func() bool { return observations.Load() > int32(retryLibraryDefaultAttempts) },
		5*time.Second,
		time.Millisecond,
		"a policy without retry.Attempts must not inherit the retry library's attempt limit",
	)
}
