package kv_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/tiny-systems/common-module/components/kv"
	"github.com/tiny-systems/common-module/internal/testharness"
)

// stateKey returns the metadata key the State backend uses for the given
// kv primary key. State stores values under "_state/<key>" base64-encoded.
const stateKeyPrefix = "_state/"

func stateKey(k string) string { return stateKeyPrefix + k }

func newKV() *testharness.Harness {
	return testharness.New((&kv.Component{}).Instance())
}

func storeDoc(t *testing.T, h *testharness.Harness, doc kv.Document) {
	t.Helper()
	result := h.Handle(context.Background(), kv.StorePort, kv.StoreRequest{
		Operation: kv.OpStore,
		Document:  doc,
	})
	if err := result.Err(); err != nil {
		t.Fatalf("store failed: %v", err)
	}
}

func queryAll(t *testing.T, h *testharness.Harness) kv.QueryResult {
	t.Helper()
	h.Reset()
	h.Handle(context.Background(), kv.QueryPort, kv.QueryRequest{})
	outs := h.PortOutputs(kv.QueryResultPort)
	if len(outs) != 1 {
		t.Fatalf("expected 1 query result, got %d", len(outs))
	}
	return outs[0].(kv.QueryResult)
}

func TestStoreAndQueryAll(t *testing.T) {
	h := newKV()
	storeDoc(t, h, kv.Document{"id": "ep1", "url": "https://example.com", "status": "UP"})

	qr := queryAll(t, h)
	if qr.Count != 1 {
		t.Fatalf("count: got %d, want 1", qr.Count)
	}
	if qr.Results[0].Key != "ep1" {
		t.Errorf("key: got %q, want ep1", qr.Results[0].Key)
	}
	if qr.Results[0].Document["status"] != "UP" {
		t.Errorf("status: got %v, want UP", qr.Results[0].Document["status"])
	}
}

func TestQueryByJSONPath(t *testing.T) {
	h := newKV()
	storeDoc(t, h, kv.Document{"id": "ep1", "status": "UP"})
	storeDoc(t, h, kv.Document{"id": "ep2", "status": "DOWN"})

	h.Reset()
	h.Handle(context.Background(), kv.QueryPort, kv.QueryRequest{
		Query: "$.status == 'DOWN'",
	})
	outs := h.PortOutputs(kv.QueryResultPort)
	if len(outs) != 1 {
		t.Fatalf("expected 1 query result output, got %d", len(outs))
	}
	qr := outs[0].(kv.QueryResult)
	if qr.Count != 1 {
		t.Fatalf("count: got %d, want 1", qr.Count)
	}
	if qr.Results[0].Key != "ep2" {
		t.Errorf("key: got %q, want ep2", qr.Results[0].Key)
	}
}

func TestDelete(t *testing.T) {
	h := newKV()
	storeDoc(t, h, kv.Document{"id": "ep1", "status": "UP"})

	h.Handle(context.Background(), kv.StorePort, kv.StoreRequest{
		Operation: kv.OpDelete,
		Document:  kv.Document{"id": "ep1"},
	})

	qr := queryAll(t, h)
	if qr.Count != 0 {
		t.Errorf("count after delete: got %d, want 0", qr.Count)
	}
}

func TestMetadataPersistence(t *testing.T) {
	h := newKV()
	storeDoc(t, h, kv.Document{"id": "ep1", "status": "UP"})

	encoded, ok := h.Metadata[stateKey("ep1")]
	if !ok {
		t.Fatalf("%s not in metadata; have: %+v", stateKey("ep1"), h.Metadata)
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("metadata value not base64: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	if doc["status"] != "UP" {
		t.Errorf("status in metadata: got %v, want UP", doc["status"])
	}
}

func TestDeleteRemovesMetadata(t *testing.T) {
	h := newKV()
	storeDoc(t, h, kv.Document{"id": "ep1", "status": "UP"})

	h.Handle(context.Background(), kv.StorePort, kv.StoreRequest{
		Operation: kv.OpDelete,
		Document:  kv.Document{"id": "ep1"},
	})

	if _, ok := h.Metadata[stateKey("ep1")]; ok {
		t.Errorf("%s should be removed from metadata after delete", stateKey("ep1"))
	}
}

func TestPodRestart(t *testing.T) {
	ctx := context.Background()
	pod1 := newKV()
	storeDoc(t, pod1, kv.Document{"id": "ep1", "status": "DOWN"})

	pod2 := pod1.NewPod()
	pod2.Reconcile(ctx)

	qr := queryAll(t, pod2)
	if qr.Count != 1 {
		t.Fatalf("pod2 count: got %d, want 1", qr.Count)
	}
	if qr.Results[0].Document["status"] != "DOWN" {
		t.Errorf("pod2 status: got %v, want DOWN", qr.Results[0].Document["status"])
	}
}

func TestStaleReconcileDoesNotOverwrite(t *testing.T) {
	ctx := context.Background()
	h := newKV()

	// Store via port sets settingsFromPort guard
	storeDoc(t, h, kv.Document{"id": "ep1", "status": "UP"})

	// Inject stale metadata and reconcile
	h.Metadata["kv-ep1"] = `{"id":"ep1","status":"STALE"}`
	h.Reconcile(ctx)

	qr := queryAll(t, h)
	if qr.Count != 1 {
		t.Fatalf("count: got %d, want 1", qr.Count)
	}
	if qr.Results[0].Document["status"] != "UP" {
		t.Errorf("stale reconcile overwrote: got %v, want UP", qr.Results[0].Document["status"])
	}
}

func TestMaxRecords(t *testing.T) {
	h := newKV()

	// Set max to 2 via settings
	h.Handle(context.Background(), "_settings", kv.Settings{
		Document:   kv.Document{"id": ""},
		PrimaryKey: "id",
		MaxRecords: 2,
	})

	storeDoc(t, h, kv.Document{"id": "a"})
	storeDoc(t, h, kv.Document{"id": "b"})

	// Third should fail
	result := h.Handle(context.Background(), kv.StorePort, kv.StoreRequest{
		Operation: kv.OpStore,
		Document:  kv.Document{"id": "c"},
	})
	err := result.Err()
	if err == nil {
		t.Fatal("expected error when store is full")
	} else if !strings.Contains(err.Error(), "store full") {
		t.Errorf("unexpected error: %v", err)
	}

	// Update existing should still work
	storeDoc(t, h, kv.Document{"id": "a", "updated": "yes"})
}

func TestDocumentTooLarge(t *testing.T) {
	h := newKV()
	bigValue := strings.Repeat("x", 33*1024) // >32KB
	result := h.Handle(context.Background(), kv.StorePort, kv.StoreRequest{
		Operation: kv.OpStore,
		Document:  kv.Document{"id": "big", "data": bigValue},
	})
	err := result.Err()
	if err == nil {
		t.Fatal("expected error for oversized document")
	} else if !strings.Contains(err.Error(), "too large") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestEmptyPrimaryKey(t *testing.T) {
	h := newKV()
	result := h.Handle(context.Background(), kv.StorePort, kv.StoreRequest{
		Operation: kv.OpStore,
		Document:  kv.Document{"id": ""},
	})
	if result.Err() == nil {
		t.Fatal("expected error for empty primary key")
	}
}

func TestMissingPrimaryKey(t *testing.T) {
	h := newKV()
	result := h.Handle(context.Background(), kv.StorePort, kv.StoreRequest{
		Operation: kv.OpStore,
		Document:  kv.Document{"name": "no-pk"},
	})
	if result.Err() == nil {
		t.Fatal("expected error for missing primary key")
	}
}

func TestUpdateExistingKey(t *testing.T) {
	h := newKV()
	storeDoc(t, h, kv.Document{"id": "ep1", "status": "UP"})
	storeDoc(t, h, kv.Document{"id": "ep1", "status": "DOWN"})

	qr := queryAll(t, h)
	if qr.Count != 1 {
		t.Fatalf("count: got %d, want 1", qr.Count)
	}
	if qr.Results[0].Document["status"] != "DOWN" {
		t.Errorf("status: got %v, want DOWN", qr.Results[0].Document["status"])
	}
}

func TestQueryEmptyStore(t *testing.T) {
	h := newKV()
	qr := queryAll(t, h)
	if qr.Count != 0 {
		t.Errorf("count: got %d, want 0", qr.Count)
	}
	if len(qr.Results) != 0 {
		t.Errorf("results: got %d items, want 0", len(qr.Results))
	}
}

func TestStoreAckPort(t *testing.T) {
	h := newKV()
	h.Handle(context.Background(), "_settings", kv.Settings{
		Document:       kv.Document{"id": ""},
		PrimaryKey:     "id",
		MaxRecords:     100,
		EnableStoreAck: true,
	})

	storeDoc(t, h, kv.Document{"id": "ep1", "status": "UP"})

	acks := h.PortOutputs(kv.StoreAckPort)
	if len(acks) != 1 {
		t.Fatalf("expected 1 ack, got %d", len(acks))
	}
	ack := acks[0].(kv.StoreAck)
	if ack.Request.Document["id"] != "ep1" {
		t.Errorf("ack document id: got %v, want ep1", ack.Request.Document["id"])
	}
}

func TestStoreAckPortHiddenByDefault(t *testing.T) {
	h := newKV()
	for _, p := range h.Ports() {
		if p.Name == kv.StoreAckPort {
			t.Fatal("store_ack port should not be visible by default")
		}
	}
}

func TestContextPassthrough(t *testing.T) {
	h := newKV()
	h.Handle(context.Background(), kv.StorePort, kv.StoreRequest{
		Context:   "my-ctx",
		Operation: kv.OpStore,
		Document:  kv.Document{"id": "ep1"},
	})

	h.Handle(context.Background(), kv.QueryPort, kv.QueryRequest{
		Context: "query-ctx",
	})

	outs := h.PortOutputs(kv.QueryResultPort)
	if len(outs) != 1 {
		t.Fatalf("expected 1 output, got %d", len(outs))
	}
	qr := outs[0].(kv.QueryResult)
	if qr.Context != "query-ctx" {
		t.Errorf("context: got %v, want query-ctx", qr.Context)
	}
}

func TestResetThenReconcileDoesNotRestore(t *testing.T) {
	ctx := context.Background()
	h := newKV()

	// Store some records
	storeDoc(t, h, kv.Document{"id": "ep1", "status": "UP"})
	storeDoc(t, h, kv.Document{"id": "ep2", "status": "DOWN"})

	// Verify 2 records
	qr := queryAll(t, h)
	if qr.Count != 2 {
		t.Fatalf("before reset: got %d, want 2", qr.Count)
	}

	// Reset via control port
	h.Reset()
	h.Handle(ctx, "_control", kv.Control{Reset: true})

	// Verify records cleared
	qr = queryAll(t, h)
	if qr.Count != 0 {
		t.Fatalf("after reset: got %d, want 0", qr.Count)
	}

	// Simulate stale reconcile (metadata not yet patched in K8s)
	h.Metadata["kv-ep1"] = `{"id":"ep1","status":"UP"}`
	h.Metadata["kv-ep2"] = `{"id":"ep2","status":"DOWN"}`
	h.Reconcile(ctx)

	// Records should still be 0 — storeUsed guard prevents reload
	qr = queryAll(t, h)
	if qr.Count != 0 {
		t.Fatalf("after stale reconcile: got %d, want 0", qr.Count)
	}
}

func TestPodRestartMultipleKeys(t *testing.T) {
	ctx := context.Background()
	pod1 := newKV()

	storeDoc(t, pod1, kv.Document{"id": "ep1", "status": "UP"})
	storeDoc(t, pod1, kv.Document{"id": "ep2", "status": "DOWN"})
	storeDoc(t, pod1, kv.Document{"id": "ep3", "status": "UP"})

	pod2 := pod1.NewPod()
	pod2.Reconcile(ctx)

	qr := queryAll(t, pod2)
	if qr.Count != 3 {
		t.Fatalf("pod2 count: got %d, want 3", qr.Count)
	}
}
