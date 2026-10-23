package sfsm_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/avast/retry-go"

	"github.com/popoffvg/sfsm"
)

type Light string

const (
	LightRed   Light = "red"
	LightGreen Light = "green"
)

type LightEvent string

func nextLight(current Light, event LightEvent) (Light, error) {
	switch {
	case current == LightRed && event == "go":
		return LightGreen, nil
	case current == LightGreen && event == "stop":
		return LightRed, nil
	}
	return current, fmt.Errorf("light: no transition from %s on %s", current, event)
}

func ExampleNewSFSM() {
	ctx := context.Background()
	light := sfsm.NewSFSM(LightRed, nextLight, &sfsm.SFSMOptions[Light, LightEvent]{
		AfterStateChanged: func(from, to Light, event LightEvent) {
			fmt.Printf("%s -> %s on %s\n", from, to, event)
		},
	})
	defer func() { _ = light.Close() }()

	_ = light.Apply(ctx, "go")
	err := light.Apply(ctx, "go")
	fmt.Println(err)
	fmt.Println(light.Get())
	// Output:
	// red -> green on go
	// light: no transition from green on go
	// green
}

func ExampleSFSM_Wait() {
	ctx := context.Background()
	light := sfsm.NewSFSM(LightRed, nextLight, nil)
	defer func() { _ = light.Close() }()

	go func() { _ = light.Apply(ctx, "go") }()

	_ = light.Wait(ctx, sfsm.Equal(LightGreen))
	fmt.Println(light.Get())
	// Output: green
}

type Deploy string

func ExampleObserveExternalState() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The deploy moves through these states outside the process.
	reports := []Deploy{"pending", "running", "running", "done"}
	var next int
	observe := func(context.Context) Deploy {
		report := reports[min(next, len(reports)-1)]
		next++
		return report
	}

	deploy := sfsm.NewSFSM(Deploy("unknown"), func(_ Deploy, observed Deploy) (Deploy, error) {
		return observed, nil
	}, nil)
	defer func() { _ = deploy.Close() }()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sfsm.ObserveExternalState(ctx, deploy, observe, retry.Delay(time.Millisecond))
	}()

	_ = deploy.Wait(ctx, sfsm.Equal(Deploy("done")))
	cancel()
	wg.Wait()
	fmt.Println(deploy.Get())
	// Output: done
}

// Order is the state a PersistedSFSM keeps in a store. Version lets the store
// refuse a write from a machine that read an older state.
type Order struct {
	Stage   string
	Version int
}

type OrderEvent string

func nextOrder(current Order, event OrderEvent) (Order, error) {
	switch {
	case current.Stage == "new" && event == "pay":
		return Order{Stage: "paid", Version: current.Version + 1}, nil
	case current.Stage == "paid" && event == "ship":
		return Order{Stage: "shipped", Version: current.Version + 1}, nil
	}
	return current, fmt.Errorf("order: no transition from %s on %s", current.Stage, event)
}

// orderStore stands in for a database table with a version column:
// UPDATE orders SET stage = $3, version = $4 WHERE id = $1 AND version = $2.
type orderStore struct {
	mu   sync.Mutex
	rows map[string]Order
}

func (s *orderStore) Load(_ context.Context, id string) (Order, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	order, ok := s.rows[id]
	if !ok {
		return Order{}, fmt.Errorf("order %s: not found", id)
	}
	return order, nil
}

func (s *orderStore) Write(_ context.Context, id string, prev, next Order) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rows[id].Version != prev.Version {
		return fmt.Errorf("order %s: %w", id, sfsm.ErrStaleState)
	}
	s.rows[id] = next
	return nil
}

func ExampleOpenPersistedSFSM() {
	ctx := context.Background()
	store := &orderStore{rows: map[string]Order{"o-1": {Stage: "new"}}}

	// One machine per request: open, apply, close.
	order, err := sfsm.OpenPersistedSFSM(ctx, store, "o-1", nextOrder, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	_ = order.Apply(ctx, "pay")
	_ = order.Close()

	fmt.Println(store.rows["o-1"].Stage)
	// Output: paid
}

func ExampleOpenPersistedSFSM_conflict() {
	ctx := context.Background()
	store := &orderStore{rows: map[string]Order{"o-1": {Stage: "paid", Version: 1}}}

	// Two requests open the same order before either one writes.
	first, _ := sfsm.OpenPersistedSFSM(ctx, store, "o-1", nextOrder, nil)
	defer func() { _ = first.Close() }()
	second, _ := sfsm.OpenPersistedSFSM(ctx, store, "o-1", nextOrder, nil)
	defer func() { _ = second.Close() }()

	fmt.Println(first.Apply(ctx, "ship"))
	err := second.Apply(ctx, "ship")
	fmt.Println(errors.Is(err, sfsm.ErrStaleState))
	fmt.Println(second.Get().Stage)
	// Output:
	// <nil>
	// true
	// paid
}
