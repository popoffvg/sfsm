# sfsm

A stateful finite state machine for Go. It serializes every change of the state through one goroutine.

```sh
go get github.com/popoffvg/sfsm
```

## Parts

- `SFSM` applies events to the state one at a time. `Apply` sends an event, `Get` reads the state, `Wait` blocks until a predicate on the state is true, `Close` stops the machine.
- `ObserveExternalState` polls an external system and applies each observation to an `SFSM`, until the context is cancelled or the machine is closed.
- `Watchable` holds a value and lets callers wait for a change of it.

## Example

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
