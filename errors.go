package sfsm

import "errors"

var (
	ErrFSMClosed    = errors.New("sfsm: machine is closed")
	errObserveAgain = errors.New("sfsm observe: observation reported, observe again")
)
