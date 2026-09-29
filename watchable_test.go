package sfsm

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWatchableGetSet(t *testing.T) {
	w := NewWatchable(0)

	require.Equal(t, 0, w.Get())

	go func() {
		w.Set(42)
	}()

	require.Eventually(t, func() bool {
		return w.Get() == 42
	}, time.Second, 10*time.Millisecond)
}

func TestWatchableWaitWithEqual(t *testing.T) {
	w := NewWatchable(0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan struct{})

	go func() {
		defer close(done)
		err := w.Wait(ctx, Equal(42))
		assert.NoError(t, err)
	}()

	time.Sleep(10 * time.Millisecond)
	w.Set(10)
	w.Set(42)

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, 42, w.Get())
}

func TestWatchableWaitContextCancel(t *testing.T) {
	w := NewWatchable(0)

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)

	started := make(chan struct{})
	go func() {
		defer wg.Done()
		close(started)
		err := w.Wait(ctx, Equal(100))
		assert.ErrorIs(t, err, context.Canceled)
	}()

	<-started
	cancel()

	wg.Wait()
}

func TestWatchableWaitTimeout(t *testing.T) {
	w := NewWatchable(0)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := w.Wait(ctx, Equal(100))
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestWatchableMultipleWaiters(t *testing.T) {
	w := NewWatchable(0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const numWaiters = 5
	var wg sync.WaitGroup
	wg.Add(numWaiters)

	for range numWaiters {
		go func() {
			defer wg.Done()
			err := w.Wait(ctx, Equal(42))
			assert.NoError(t, err)
		}()
	}

	time.Sleep(10 * time.Millisecond)
	w.Set(42)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, 42, w.Get())
}

func TestWatchableConcurrentSetGet(t *testing.T) {
	w := NewWatchable(0)

	const numGoroutines = 10
	var wg sync.WaitGroup

	for i := range numGoroutines {
		wg.Go(func() {
			w.Set(i)
		})
	}

	for range numGoroutines {
		wg.Go(func() {
			_ = w.Get()
		})
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond)
}

func TestWatchableCustomPredicate(t *testing.T) {
	w := NewWatchable(0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	greaterThan10 := func(prev, current int) bool {
		return current > 10
	}

	done := make(chan struct{})

	go func() {
		defer close(done)
		err := w.Wait(ctx, greaterThan10)
		assert.NoError(t, err)
	}()

	time.Sleep(10 * time.Millisecond)
	w.Set(5)
	time.Sleep(10 * time.Millisecond)
	w.Set(8)
	time.Sleep(10 * time.Millisecond)
	w.Set(15)

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, 15, w.Get())
}

func TestWatchablePredicateWithPreviousValue(t *testing.T) {
	w := NewWatchable(0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	increasedBy5 := func(prev, current int) bool {
		return current-prev == 5
	}

	done := make(chan struct{})

	go func() {
		defer close(done)
		err := w.Wait(ctx, increasedBy5)
		assert.NoError(t, err)
	}()

	time.Sleep(10 * time.Millisecond)
	w.Set(2)
	time.Sleep(10 * time.Millisecond)
	w.Set(5)
	time.Sleep(10 * time.Millisecond)
	w.Set(10)

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, 10, w.Get())
}

func TestWatchableStringType(t *testing.T) {
	w := NewWatchable("")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan struct{})

	go func() {
		defer close(done)
		err := w.Wait(ctx, Equal("hello"))
		assert.NoError(t, err)
	}()

	time.Sleep(10 * time.Millisecond)
	w.Set("world")
	w.Set("hello")

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, "hello", w.Get())
}

func TestWatchableImmediatelySatisfied(t *testing.T) {
	w := NewWatchable(42)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Go(func() {
		w.Set(42)
	})

	err := w.Wait(ctx, Equal(42))
	wg.Wait()
	require.NoError(t, err)
}
