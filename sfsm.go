package sfsm

import (
	"context"
	"fmt"
)

type eventRequest[Event any] struct {
	event         Event
	processResult chan error
}

type SFSMOptions[State comparable, Event any] struct {
	AfterStateChanged func(State, State, Event)
}

// SFSM is a stateful finite state machine. It is used to serialize access to the state.
//
// OnProcess callback is called to process the event. During OnProcess call the exclusive access to the state is
// not guaranteed.
// SFSM supposes that state is changed only using SFSM.Apply() method.
//
// AfterStateChanged callback is called after the state is changed.
// The order of calls AfterStateChanged and OnProcess is not guaranteed.
type SFSM[State comparable, Event any] struct {
	state *Watchable[State]

	onProcess         func(currentState State, event Event) (newState State, err error)
	afterStateChanged func(fromState, toState State, event Event)
	eventCh           chan eventRequest[Event]
	stopCh            chan struct{}
	doneCh            chan struct{}
}

// NewSFSM creates a new stateful finite state machine.
//
// Example:
//
//	type State string
//	const (
//		StateIdle    State = "idle"
//		StateRunning State = "running"
//		StateDone    State = "done"
//	)
//
//	type Event string
//	const (
//		EventStart Event = "start"
//		EventStop  Event = "stop"
//	)
//
//	fsm := NewSFSM(StateIdle, func(state State, event Event) (State, error) {
//		switch state {
//		case StateIdle:
//			if event == EventStart {
//				return StateRunning, nil
//			}
//		case StateRunning:
//			if event == EventStop {
//				return StateDone, nil
//			}
//		}
//		return state, nil // no state change
//	}, &SFSMOptions[State, Event]{
//		AfterStateChanged: func(prev, next State, event Event) {
//			log.Printf("State changed: %s -> %s (event: %s)", prev, next, event)
//		},
//	})
//
//	ctx := context.Background()
//	fsm.Apply(ctx, EventStart) // transitions to StateRunning
//	fsm.Wait(ctx, func(prev, current State) bool {
//		return current == StateDone
//	}) // waits until state becomes StateDone
//	defer fsm.Close()
func NewSFSM[State comparable, Event any](
	initialState State,
	onProcess func(State, Event) (State, error),
	opts *SFSMOptions[State, Event],
) *SFSM[State, Event] {
	doneCh := make(chan struct{})

	fsm := &SFSM[State, Event]{
		state:     newWatchableWithCancelCh(initialState, doneCh),
		onProcess: onProcess,
		eventCh:   make(chan eventRequest[Event]),
		stopCh:    make(chan struct{}),
		doneCh:    doneCh,
	}

	if opts != nil {
		fsm.afterStateChanged = opts.AfterStateChanged
	}

	go fsm.processEvents()
	return fsm
}

func (s *SFSM[State, Event]) processEvents() {
	defer close(s.doneCh)

	for {
		select {
		case <-s.stopCh:
			return
		case req := <-s.eventCh:
			// we can copy directly without implement DeepCopy because type is comparable
			currentState := s.state.Get()
			newState, err := s.onProcess(currentState, req.event)
			if err != nil {
				req.processResult <- err
				continue
			}

			if currentState == newState {
				req.processResult <- nil
				continue
			}

			s.state.Set(newState)

			if s.afterStateChanged != nil {
				s.afterStateChanged(currentState, newState, req.event)
			}

			req.processResult <- nil
		}
	}
}

func (s *SFSM[State, Event]) Apply(ctx context.Context, event Event) error {
	resultCh := make(chan error, 1)
	req := eventRequest[Event]{
		event:         event,
		processResult: resultCh,
	}

	select {
	case <-s.stopCh:
		return ErrFSMClosed
	case <-ctx.Done():
		return fmt.Errorf("sfsm: apply failed (send event): %w", ctx.Err())
	case s.eventCh <- req:
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("sfsm: apply failed (wait for result): %w", ctx.Err())
	case err := <-resultCh:
		return err
	}
}

func (s *SFSM[State, Event]) Get() State {
	return s.state.Get()
}

func (s *SFSM[State, Event]) Wait(ctx context.Context, condition predicate[State]) error {
	return s.state.Wait(ctx, condition)
}

func (s *SFSM[State, Event]) Close() error {
	close(s.stopCh)
	<-s.doneCh
	return nil
}
