package sfsm

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type ctxKey struct{}

type recordingStore struct {
	state    testState
	writeErr error
	writes   int
	lastCtx  context.Context
}

func (s *recordingStore) Load(context.Context, string) (testState, error) {
	return s.state, nil
}

func (s *recordingStore) Write(ctx context.Context, _ string, prev, next testState) error {
	s.lastCtx = ctx
	if s.writeErr != nil {
		return s.writeErr
	}
	if prev != s.state {
		return ErrStaleState
	}
	s.writes++
	s.state = next
	return nil
}

func startOnly(current testState, event testEvent) (testState, error) {
	if current == stateInit && event.action == "start" {
		return stateRunning, nil
	}
	return current, nil
}

func TestPersistedSFSM_WritesTheNewState(t *testing.T) {
	store := &recordingStore{state: stateInit}
	machine, err := OpenPersistedSFSM(context.Background(), store, "id", startOnly, nil)
	require.NoError(t, err)
	defer func() { _ = machine.Close() }()

	ctx := context.WithValue(context.Background(), ctxKey{}, "request")
	require.NoError(t, machine.Apply(ctx, testEvent{action: "start"}))

	require.Equal(t, stateRunning, store.state)
	require.Equal(t, stateRunning, machine.Get())
	require.Equal(t, "request", store.lastCtx.Value(ctxKey{}))
}

func TestPersistedSFSM_FailedWriteKeepsTheState(t *testing.T) {
	store := &recordingStore{state: stateInit, writeErr: errors.New("disk full")}
	machine, err := OpenPersistedSFSM(context.Background(), store, "id", startOnly, nil)
	require.NoError(t, err)
	defer func() { _ = machine.Close() }()

	err = machine.Apply(context.Background(), testEvent{action: "start"})

	require.ErrorContains(t, err, "disk full")
	require.Equal(t, stateInit, machine.Get())
}

func TestPersistedSFSM_NoWriteWhenStateIsSame(t *testing.T) {
	store := &recordingStore{state: stateInit}
	machine, err := OpenPersistedSFSM(context.Background(), store, "id", startOnly, nil)
	require.NoError(t, err)
	defer func() { _ = machine.Close() }()

	require.NoError(t, machine.Apply(context.Background(), testEvent{action: "noop"}))
	require.Zero(t, store.writes)
}

func TestPersistedSFSM_AfterStateChangedGetsTheEvent(t *testing.T) {
	store := &recordingStore{state: stateInit}
	var seen testEvent
	machine, err := OpenPersistedSFSM(context.Background(), store, "id", startOnly,
		&SFSMOptions[testState, testEvent]{
			AfterStateChanged: func(_, _ testState, event testEvent) { seen = event },
		})
	require.NoError(t, err)

	require.NoError(t, machine.Apply(context.Background(), testEvent{action: "start"}))
	require.NoError(t, machine.Close())
	require.Equal(t, "start", seen.action)
}
