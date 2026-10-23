package sfsm

import (
	"context"
	"fmt"
)

// Store loads and writes the state of one PersistedSFSM.
//
// Write MUST fail when the stored state is no longer prev, so two machines that
// opened the same id cannot both move it. Wrap ErrStaleState for that failure.
// Write MAY delete the record when next is a final state.
type Store[ID any, State comparable] interface {
	Load(ctx context.Context, id ID) (State, error)
	Write(ctx context.Context, id ID, prev, next State) error
}

type persistedEvent[Event any] struct {
	ctx   context.Context
	event Event
}

// PersistedSFSM is an SFSM whose state lives in a Store. A transition is kept
// only when the Store accepts the write; on a failed write the state in memory
// stays as it was. It holds one id for a short time, such as one request.
type PersistedSFSM[ID any, State comparable, Event any] struct {
	id      ID
	machine *SFSM[State, persistedEvent[Event]]
}

// OpenPersistedSFSM loads the state of id from store and starts a machine on it.
func OpenPersistedSFSM[ID any, State comparable, Event any](
	ctx context.Context,
	store Store[ID, State],
	id ID,
	onProcess func(State, Event) (State, error),
	opts *SFSMOptions[State, Event],
) (*PersistedSFSM[ID, State, Event], error) {
	initialState, err := store.Load(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("sfsm: open persisted (load): %w", err)
	}

	transition := func(current State, req persistedEvent[Event]) (State, error) {
		next, err := onProcess(current, req.event)
		if err != nil || next == current {
			return next, err
		}
		if err := store.Write(req.ctx, id, current, next); err != nil {
			return current, fmt.Errorf("sfsm: persisted write: %w", err)
		}
		return next, nil
	}

	var innerOpts *SFSMOptions[State, persistedEvent[Event]]
	if opts != nil && opts.AfterStateChanged != nil {
		afterStateChanged := opts.AfterStateChanged
		innerOpts = &SFSMOptions[State, persistedEvent[Event]]{
			AfterStateChanged: func(from, to State, req persistedEvent[Event]) {
				afterStateChanged(from, to, req.event)
			},
		}
	}

	return &PersistedSFSM[ID, State, Event]{
		id:      id,
		machine: NewSFSM(initialState, transition, innerOpts),
	}, nil
}

// Apply processes event and writes the new state with ctx.
func (p *PersistedSFSM[ID, State, Event]) Apply(ctx context.Context, event Event) error {
	return p.machine.Apply(ctx, persistedEvent[Event]{ctx: ctx, event: event})
}

func (p *PersistedSFSM[ID, State, Event]) ID() ID {
	return p.id
}

// Get returns the last state this machine loaded or wrote. Another machine on
// the same id can have moved the stored state since.
func (p *PersistedSFSM[ID, State, Event]) Get() State {
	return p.machine.Get()
}

func (p *PersistedSFSM[ID, State, Event]) Close() error {
	return p.machine.Close()
}
