package sfsm

import "errors"

var (
	ErrFSMClosed = errors.New("sfsm: machine is closed")
	// ErrStaleState is what a Store wraps when the stored state is no longer the one the machine read.
	ErrStaleState   = errors.New("sfsm: stored state changed")
	errObserveAgain = errors.New("sfsm observe: observation reported, observe again")
)
