// Package pipeline is the example for a saga whose steps feed each other: the
// reservation id goes into the charge, and the charge id goes into the
// shipment.
//
// The point is what that does to the compensations. A compensation is given the
// same input as the step it undoes, so it gets the upstream ids for free --
// which matters, because the compensation is registered before the step runs
// and therefore cannot see that step's output.
package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/yamakura-yuma/temporal-saga/saga"
)

// TaskQueue is shared between the worker and whoever starts the workflow.
const TaskQueue = "saga-pipeline"

// Order is the workflow input.
type Order struct {
	ID     string `json:"id"`
	SKU    string `json:"sku"`
	Amount int    `json:"amount"`
	// FailAt names a step whose forward call should fail.
	FailAt string `json:"fail_at,omitempty"`
}

// Receipt is the workflow output.
type Receipt struct {
	Reservation string `json:"reservation"`
	Charge      string `json:"charge"`
	Shipment    string `json:"shipment"`
}

// Each request carries what the step before it produced. The compensation of a
// step receives the same struct, so it can undo the work against the upstream
// id rather than having to look it up again.
type (
	ReserveReq struct {
		Order Order `json:"order"`
	}
	ChargeReq struct {
		Order Order `json:"order"`
		// Reservation is the id the reserve step returned.
		Reservation string `json:"reservation"`
	}
	ShipReq struct {
		Order Order `json:"order"`
		// Charge is the id the charge step returned.
		Charge string `json:"charge"`
	}
)

// PipelineWorkflow reserves stock, charges against that reservation, and ships
// against that charge.
func PipelineWorkflow(ctx workflow.Context, in Order) (Receipt, error) {
	var a *Activities

	return saga.Run(ctx, saga.Options{
		ActivityOptions: workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		},
		CompensationBudget: time.Minute,
	}, func(ctx workflow.Context, s *saga.Saga) (Receipt, error) {
		res, _ := saga.Step(ctx, s, "reserve", saga.Activity(a.Reserve, a.Unreserve), ReserveReq{Order: in})

		chg, _ := saga.Step(ctx, s, "charge", saga.Activity(a.Charge, a.Refund), ChargeReq{Order: in, Reservation: res})

		shp, _ := saga.Step(ctx, s, "ship", saga.Activity(a.Ship, a.CancelShipment), ShipReq{Order: in, Charge: chg})

		return Receipt{Reservation: res, Charge: chg, Shipment: shp}, nil
	})
}

// Ledger records what each activity did, and what each compensation saw.
type Ledger struct {
	mu     sync.Mutex
	claims map[string]string // idempotency key -> the id handed out
	saw    map[string]string // step name -> the upstream id its compensation got
}

// NewLedger returns an empty Ledger.
func NewLedger() *Ledger {
	return &Ledger{claims: map[string]string{}, saw: map[string]string{}}
}

func (l *Ledger) claim(key, id string) (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if existing, ok := l.claims[key]; ok {
		return existing, false
	}
	l.claims[key] = id
	return id, true
}

func (l *Ledger) release(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.claims[key]; !ok {
		return false
	}
	delete(l.claims, key)
	return true
}

// record notes the upstream id a compensation was handed.
func (l *Ledger) record(step, upstream string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.saw[step] = upstream
}

// Upstream reports the id the named step's compensation received. A
// specification uses it to check that the pipeline really did feed it through.
func (l *Ledger) Upstream(step string) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.saw[step]
}

// Held reports whether a step of a given workflow run still holds its claim.
func (l *Ledger) Held(runID, step string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.claims[runID+"/"+step]
	return ok
}

// Activities is the worked example. NewActivities takes the ledger so a test
// can look at it.
type Activities struct{ ledger *Ledger }

// NewActivities returns activities backed by the given ledger.
func NewActivities(ledger *Ledger) *Activities { return &Activities{ledger: ledger} }

func key(ctx context.Context, fallback string) string {
	if k, ok := saga.IdempotencyKey(ctx); ok {
		return k
	}
	return fallback
}

// Reserve holds stock.
func (a *Activities) Reserve(ctx context.Context, req ReserveReq) (string, error) {
	k := key(ctx, "reserve/"+req.Order.ID)
	id, fresh := a.ledger.claim(k, "res-"+req.Order.ID)
	if !fresh {
		return id, nil
	}
	if req.Order.FailAt == "reserve" {
		a.ledger.release(k)
		return "", fmt.Errorf("reserve: no stock for %s", req.Order.SKU)
	}
	return id, nil
}

// Unreserve releases the stock Reserve held.
func (a *Activities) Unreserve(ctx context.Context, req ReserveReq) error {
	k := key(ctx, "reserve/"+req.Order.ID)
	if !a.ledger.release(k) {
		return nil
	}
	logf(ctx, "released the stock for %s", req.Order.ID)
	return nil
}

// Charge takes payment against the reservation.
func (a *Activities) Charge(ctx context.Context, req ChargeReq) (string, error) {
	k := key(ctx, "charge/"+req.Order.ID)
	id, fresh := a.ledger.claim(k, "chg-"+req.Reservation)
	if !fresh {
		return id, nil
	}
	if req.Order.FailAt == "charge" {
		a.ledger.release(k)
		return "", fmt.Errorf("charge: card declined for %s", req.Order.ID)
	}
	logf(ctx, "charged %d against reservation %s", req.Order.Amount, req.Reservation)
	return id, nil
}

// Refund reverses Charge. It knows which reservation the charge belonged to
// because the saga handed the compensation the same input the step got.
func (a *Activities) Refund(ctx context.Context, req ChargeReq) error {
	a.ledger.record("charge", req.Reservation)

	k := key(ctx, "charge/"+req.Order.ID)
	if !a.ledger.release(k) {
		return nil
	}
	logf(ctx, "refunded the charge against reservation %s", req.Reservation)
	return nil
}

// Ship books a shipment against the charge.
func (a *Activities) Ship(ctx context.Context, req ShipReq) (string, error) {
	k := key(ctx, "ship/"+req.Order.ID)
	id, fresh := a.ledger.claim(k, "shp-"+req.Charge)
	if !fresh {
		return id, nil
	}
	if req.Order.FailAt == "ship" {
		a.ledger.release(k)
		return "", fmt.Errorf("ship: no carrier available for %s", req.Order.ID)
	}
	return id, nil
}

// CancelShipment reverses Ship.
func (a *Activities) CancelShipment(ctx context.Context, req ShipReq) error {
	a.ledger.record("ship", req.Charge)

	k := key(ctx, "ship/"+req.Order.ID)
	if !a.ledger.release(k) {
		return nil
	}
	logf(ctx, "cancelled the shipment for charge %s", req.Charge)
	return nil
}

func logf(ctx context.Context, format string, args ...any) {
	defer func() { _ = recover() }()
	activity.GetLogger(ctx).Info(fmt.Sprintf(format, args...))
}
