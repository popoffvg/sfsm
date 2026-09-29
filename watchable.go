package sfsm

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type predicate[T comparable] func(prev, current T) bool

func Equal[T comparable](expected T) predicate[T] {
	return func(_, current T) bool {
		return current == expected
	}
}

type changes[T comparable] struct {
	prevValue    T
	currentValue T
}

type subscriber[T comparable] struct {
	ch chan changes[T]
}

type Watchable[T comparable] struct {
	mu          sync.RWMutex
	value       T
	subscribers map[*subscriber[T]]struct{}

	cancelCh <-chan struct{} // cancels Wait() without waiting for context
}

func NewWatchable[T comparable](defaultValue T) *Watchable[T] {
	return &Watchable[T]{
		value:       defaultValue,
		subscribers: make(map[*subscriber[T]]struct{}),
	}
}

func newWatchableWithCancelCh[T comparable](defaultValue T, stopCh <-chan struct{}) *Watchable[T] {
	w := NewWatchable(defaultValue)
	w.cancelCh = stopCh
	return w
}

func (w *Watchable[T]) Set(value T) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.unsafeSet(value)
}

// unsafeSet is set without locking the mutex.
// The caller is responsible to lock the mutex before calling this method.
func (w *Watchable[T]) unsafeSet(value T) {
	prevValue := w.value
	w.value = value
	change := changes[T]{prevValue: prevValue, currentValue: value}

	// Not fire the event
	if prevValue == value {
		return
	}

	for sub := range w.subscribers {
		sub.ch <- change
	}
}

func (w *Watchable[T]) Get() T {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.value
}

func (w *Watchable[T]) subscribe() *subscriber[T] {
	sub := &subscriber[T]{
		ch: make(chan changes[T], 1),
	}
	w.subscribers[sub] = struct{}{}
	return sub
}

func (w *Watchable[T]) unsubscribe(sub *subscriber[T]) {
	w.mu.Lock()
	defer w.mu.Unlock()

	delete(w.subscribers, sub)
	close(sub.ch)
}

// Wait waits for the predicate to be satisfied.
// It returns when the predicate returns true or when the context is cancelled.
func (w *Watchable[T]) Wait(ctx context.Context, condition predicate[T]) error {
	w.mu.Lock()

	currentValue := w.value

	if condition(currentValue, currentValue) {
		w.mu.Unlock()
		return nil
	}

	sub := w.subscribe()

	w.mu.Unlock()
	defer w.unsubscribe(sub)

	for {
		select {
		case <-w.cancelCh:
			return ErrFSMClosed // special case: if FSM get stopped before condition, we should exit without waiting for context
		case <-ctx.Done():
			return fmt.Errorf("sfsm: wait failed: %w", ctx.Err())
		case change, ok := <-sub.ch:
			if !ok {
				return errors.New("sfsm: unexpected close of watchable channel")
			}
			if condition(change.prevValue, change.currentValue) {
				return nil
			}
		}
	}
}
