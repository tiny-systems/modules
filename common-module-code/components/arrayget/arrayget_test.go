package arrayget

import (
	"context"
	"errors"
	"testing"

	"github.com/tiny-systems/module/module"
)

func TestArrayGet_ValidIndex(t *testing.T) {
	c := &Component{}
	items := []Item{"apple", "banana", "cherry"}

	var result Result
	handler := module.Handler(func(_ context.Context, port string, msg any) module.Result {
		if port == ResultPort {
			result = msg.(Result)
		}
		return module.Result{}
	})

	r := c.handleRequest(context.Background(), handler, Request{
		Array: items,
		Index: 2,
	})
	if err := r.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Item != "banana" {
		t.Fatalf("expected banana, got %v", result.Item)
	}
	if result.Index != 2 {
		t.Fatalf("expected index 2, got %d", result.Index)
	}
}

func TestArrayGet_FirstItem(t *testing.T) {
	c := &Component{}
	items := []Item{"only"}

	var result Result
	handler := module.Handler(func(_ context.Context, port string, msg any) module.Result {
		if port == ResultPort {
			result = msg.(Result)
		}
		return module.Result{}
	})

	r := c.handleRequest(context.Background(), handler, Request{
		Array: items,
		Index: 1,
	})
	if err := r.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Item != "only" {
		t.Fatalf("expected 'only', got %v", result.Item)
	}
}

func TestArrayGet_EmptyArray(t *testing.T) {
	c := &Component{}

	r := c.handleRequest(context.Background(), nil, Request{
		Array: []Item{},
		Index: 1,
	})
	if r.Err() == nil {
		t.Fatal("expected error for empty array")
	}
}

func TestArrayGet_NilArray(t *testing.T) {
	c := &Component{}

	r := c.handleRequest(context.Background(), nil, Request{
		Array: nil,
		Index: 1,
	})
	if r.Err() == nil {
		t.Fatal("expected error for nil array")
	}
}

func TestArrayGet_IndexTooHigh(t *testing.T) {
	c := &Component{}

	r := c.handleRequest(context.Background(), nil, Request{
		Array: []Item{"a", "b"},
		Index: 5,
	})
	if r.Err() == nil {
		t.Fatal("expected error for index out of range")
	}
}

func TestArrayGet_IndexZero(t *testing.T) {
	c := &Component{}

	r := c.handleRequest(context.Background(), nil, Request{
		Array: []Item{"a"},
		Index: 0,
	})
	if r.Err() == nil {
		t.Fatal("expected error for index 0")
	}
}

func TestArrayGet_IndexNegative(t *testing.T) {
	c := &Component{}

	r := c.handleRequest(context.Background(), nil, Request{
		Array: []Item{"a"},
		Index: -1,
	})
	if r.Err() == nil {
		t.Fatal("expected error for negative index")
	}
}

func TestArrayGet_ErrorPort(t *testing.T) {
	c := &Component{settings: Settings{EnableErrorPort: true}}

	var gotError Error
	handler := module.Handler(func(_ context.Context, port string, msg any) module.Result {
		if port == ErrorPort {
			gotError = msg.(Error)
		}
		return module.Result{}
	})

	r := c.handleRequest(context.Background(), handler, Request{
		Array: []Item{},
		Index: 1,
	})
	if err := r.Err(); err != nil {
		t.Fatalf("with error port enabled, should not return error: %v", err)
	}
	if gotError.Error == "" {
		t.Fatal("expected error message on error port")
	}
}

func TestArrayGet_ContextPassthrough(t *testing.T) {
	c := &Component{}
	ctx := map[string]string{"key": "value"}

	var result Result
	handler := module.Handler(func(_ context.Context, port string, msg any) module.Result {
		if port == ResultPort {
			result = msg.(Result)
		}
		return module.Result{}
	})

	r := c.handleRequest(context.Background(), handler, Request{
		Context: ctx,
		Array:   []Item{"item"},
		Index:   1,
	})
	if err := r.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ctxMap, ok := result.Context.(map[string]string)
	if !ok {
		t.Fatal("context should be map[string]string")
	}
	if ctxMap["key"] != "value" {
		t.Fatal("context not passed through")
	}
}

func TestArrayGet_HandlerErrorPropagation(t *testing.T) {
	c := &Component{}
	expectedErr := errors.New("downstream error")

	handler := module.Handler(func(_ context.Context, port string, msg any) module.Result {
		return module.Fail(expectedErr)
	})

	r := c.handleRequest(context.Background(), handler, Request{
		Array: []Item{"item"},
		Index: 1,
	})
	if !errors.Is(r.Err(), expectedErr) {
		t.Fatalf("expected handler error to propagate, got: %v", r.Err())
	}
}
