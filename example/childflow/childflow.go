// Package childflow is the example for a step that is a child workflow rather
// than an activity.
//
// A saga can mix them, and so can a single step. Here packing is a child
// workflow because it is long enough to deserve its own history, while undoing
// it is one activity call, and reserving and shipping stay activities
// throughout. The rollback runs them all in one reverse order.
//
// The child reads its idempotency key with saga.IdempotencyKeyOf, which is the
// workflow-side twin of saga.IdempotencyKey. The key rides in the child's
// WorkflowID, and the compensating child gets the same key back with the
// ":undo" suffix stripped.
package childflow

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

// TaskQueue is shared between the worker and whoever starts the workflow. The
// children inherit it.
const TaskQueue = "saga-childflow"

// Order is the workflow input.
type Order struct {
	ID     string `json:"id"`
	SKU    string `json:"sku"`
	FailAt string `json:"fail_at,omitempty"`
}

// Receipt is the workflow output.
type Receipt struct {
	Reservation string `json:"reservation"`
	Pack        string `json:"pack"`
	Shipment    string `json:"shipment"`
}

type (
	ReserveReq struct {
		Order Order `json:"order"`
	}
	PackReq struct {
		Order Order `json:"order"`
	}
	ShipReq struct {
		Order Order  `json:"order"`
		Pack  string `json:"pack"`
	}
)

// ChildflowWorkflow reserves stock, packs the order in a child workflow, and
// books a shipment.
func ChildflowWorkflow(ctx workflow.Context, in Order) (Receipt, error) {
	var a *Activities

	return saga.Run(ctx, saga.Options{
		ActivityOptions: workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		},
		CompensationBudget: time.Minute,
	}, func(ctx workflow.Context, s *saga.Saga) (Receipt, error) {
		res, _ := saga.Step(ctx, s, "reserve", saga.Activity(a.Reserve), saga.UndoActivity(a.Unreserve), ReserveReq{Order: in})

		// The two halves run differently. Packing is long enough to deserve a
		// child workflow of its own; undoing it is one activity call. Both
		// still see the same idempotency key -- the child reads it with
		// IdempotencyKeyOf, the activity with IdempotencyKey.
		pack, _ := saga.Step(ctx, s, "pack",
			saga.ChildWorkflow(PackWorkflow), saga.UndoActivity(a.Unpack),
			PackReq{Order: in})

		shp, _ := saga.Step(ctx, s, "ship", saga.Activity(a.Ship), saga.UndoActivity(a.CancelShipment), ShipReq{Order: in, Pack: pack})

		return Receipt{Reservation: res, Pack: pack, Shipment: shp}, nil
	})
}

// PackWorkflow is the forward half of the packing step.
func PackWorkflow(ctx workflow.Context, req PackReq) (string, error) {
	var a *Activities

	key, _ := saga.IdempotencyKeyOf(ctx)
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})

	var id string
	err := workflow.ExecuteActivity(ctx, a.Pack, Note{Key: key, Order: req.Order.ID}).Get(ctx, &id)
	return id, err
}

// Note is what the packing children hand their activities.
type Note struct {
	Key   string `json:"key"`
	Order string `json:"order"`
}

// Ledger records what happened, including the keys the packing children saw.
type Ledger struct {
	mu     sync.Mutex
	claims map[string]string
	keys   map[string]string // "pack" / "unpack" -> the key that child read
}

// NewLedger returns an empty Ledger.
func NewLedger() *Ledger {
	return &Ledger{claims: map[string]string{}, keys: map[string]string{}}
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

func (l *Ledger) noteKey(which, key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.keys[which] = key
}

// KeySeenBy reports the idempotency key the named packing child read. A
// specification uses it to check both halves saw the same one.
func (l *Ledger) KeySeenBy(which string) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.keys[which]
}

// Held reports whether a step still holds its claim.
func (l *Ledger) Held(runID, step string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.claims[runID+"/"+step]
	return ok
}

// Activities backs both the saga's activity steps and the packing children.
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
	a.ledger.release(key(ctx, "reserve/"+req.Order.ID))
	return nil
}

// Pack is what the packing child does. The key comes from the child, not from
// this activity's own id.
func (a *Activities) Pack(ctx context.Context, note Note) (string, error) {
	a.ledger.noteKey("pack", note.Key)

	id, fresh := a.ledger.claim(note.Key, "pk-"+note.Order)
	if !fresh {
		return id, nil
	}
	logf(ctx, "packed %s under key %s", note.Order, note.Key)
	return id, nil
}

// Unpack undoes Pack, and succeeds when there is nothing packed.
//
// It is the compensation of a step whose forward half was a child workflow, and
// it still reads the same key -- here with IdempotencyKey, because this is an
// activity, where the child used IdempotencyKeyOf.
func (a *Activities) Unpack(ctx context.Context, req PackReq) error {
	k := key(ctx, "pack/"+req.Order.ID)
	a.ledger.noteKey("unpack", k)

	if !a.ledger.release(k) {
		logf(ctx, "unpack: nothing packed under key %s, nothing to do", k)
		return nil
	}
	logf(ctx, "unpacked %s under key %s", req.Order.ID, k)
	return nil
}

// Ship books a shipment.
func (a *Activities) Ship(ctx context.Context, req ShipReq) (string, error) {
	k := key(ctx, "ship/"+req.Order.ID)
	id, fresh := a.ledger.claim(k, "shp-"+req.Order.ID)
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
	a.ledger.release(key(ctx, "ship/"+req.Order.ID))
	return nil
}

func logf(ctx context.Context, format string, args ...any) {
	defer func() { _ = recover() }()
	activity.GetLogger(ctx).Info(fmt.Sprintf(format, args...))
}
