package sfsm

import (
	"context"
	"errors"
	"math"

	"github.com/avast/retry-go"
)

type (
	// RetryOption paces the observations and may cap their number; an option given
	// here wins over the unlimited default, so a policy without retry.Attempts still
	// observes until ctx is cancelled or the machine is closed.
	RetryOption = retry.Option
)

// ObserveExternalState polls an external system through observe and applies every
// observation to fsm, until ctx is cancelled or fsm is closed. It blocks for that
// whole time, and reports nothing: the observed state is in fsm.
//
// Only ErrFSMClosed ends the poll. Any other error from fsm.Apply leaves the state
// as it was and the next observation replaces the rejected one, so one invalid or
// transient event cannot stop the synchronisation for good.
func ObserveExternalState[State comparable, Event any](
	ctx context.Context,
	fsm *SFSM[State, Event],
	observe func(ctx context.Context) Event,
	policy ...RetryOption,
) {
	observing, stopObserving := context.WithCancel(ctx)
	defer stopObserving()

	// retry.Do reads the context only after the pause, so a machine closed mid-pause
	// would hold the caller for one more pause without this bridge.
	go func() {
		select {
		case <-fsm.stopCh:
			stopObserving()
		case <-observing.Done():
		}
	}()

	options := make([]RetryOption, 0, len(policy)+5)
	options = append(
		options,
		retry.Attempts(math.MaxUint),
		retry.Delay(retry.DefaultDelay),
		// retry-go backs off by default, which would stretch a long poll to hours.
		retry.DelayType(retry.FixedDelay),
	)
	options = append(options, policy...)
	options = append(
		options,
		retry.Context(observing),
		retry.LastErrorOnly(true),
	)

	_ = retry.Do(
		func() error {
			select {
			case <-fsm.stopCh:
				return retry.Unrecoverable(ErrFSMClosed)
			case <-observing.Done():
				return retry.Unrecoverable(observing.Err())
			default:
			}

			err := fsm.Apply(observing, observe(observing))
			if errors.Is(err, ErrFSMClosed) {
				return retry.Unrecoverable(err)
			}

			// A nil error would end the poll after one observation, so a done
			// observation must report one.
			return errObserveAgain
		},
		options...,
	)
}
