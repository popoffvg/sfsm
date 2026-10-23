# sfsm

A stateful finite state machine for Go. It serializes every change of the state through one goroutine.

```sh
go get github.com/popoffvg/sfsm
```

## Parts

- `SFSM` applies events to the state one at a time. `Apply` sends an event, `Get` reads the state, `Wait` blocks until a predicate on the state is true, `Close` stops the machine.
- `ObserveExternalState` polls an external system and applies each observation to an `SFSM`, until the context is cancelled or the machine is closed.
- `PersistedSFSM` keeps the state in a `Store`. `OpenPersistedSFSM` loads it, and `Apply` writes each new state with the context of that call. A failed write keeps the old state. A `Store` refuses a write over a state it no longer holds and wraps `ErrStaleState`, so two machines on one id cannot both move it.
- `Watchable` holds a value and lets callers wait for a change of it.

## Example

More examples run as tests in `example_test.go`: a wait, a poll with `ObserveExternalState`, and a `PersistedSFSM` with a version check.

```go
type State string
type Event string

fsm := sfsm.NewSFSM(State("idle"), func(s State, e Event) (State, error) {
	if s == "idle" && e == "start" {
		return "running", nil
	}
	return s, nil
}, nil)
defer fsm.Close()

_ = fsm.Apply(ctx, "start")
_ = fsm.Wait(ctx, sfsm.Equal(State("running")))
```
