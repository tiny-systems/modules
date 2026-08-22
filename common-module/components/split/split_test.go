package split

import (
	"context"
	"reflect"
	"testing"

	"github.com/tiny-systems/module/module"
)

type h func(ctx context.Context, handler module.Handler, port string, msg interface{}) module.Result

func TestSplit_Handle(t1 *testing.T) {
	tests := []struct {
		name       string
		handleFunc func(t1 *testing.T, handle h) module.Result
		wantErr    bool
	}{
		{
			name:    "test invalid message",
			wantErr: true,
			handleFunc: func(t *testing.T, handle h) module.Result {
				return handle(context.Background(), func(ctx context.Context, port string, data interface{}) module.Result {
					// should not be triggered cause message is invalid
					t.Error("output handler should not be triggered if income message is invalid")
					return module.Result{}
				}, "", 1)
			},
		},
		{
			name:    "OK empty message",
			wantErr: false,
			handleFunc: func(t *testing.T, handle h) module.Result {
				return handle(context.Background(), func(ctx context.Context, port string, data interface{}) module.Result {
					// should not be triggered cause message is invalid
					t.Error("output handler should not be triggered if income message is empty")
					return module.Result{}
				}, "", InMessage{})
			},
		},
		{
			name: "OK message",
			handleFunc: func(t *testing.T, handle h) module.Result {
				var counter int

				var msg = InMessage{
					Array: []ItemContext{
						1, 2, 5,
					},
					Context: 42,
				}
				var length = len(msg.Array)

				defer func() {
					if counter != length {
						t.Errorf("handler should be trigged exactly %d times, but insted called %d", length, counter)
					}
				}()

				var resp = func(ctx context.Context, port string, data interface{}) module.Result {
					if port != OutPort {
						t1.Fatalf("invalid output port: %v", port)
					}
					resp, ok := data.(OutMessage)
					if !ok {
						t1.Fatalf("invalid type of response: %v", resp)
					}
					if !reflect.DeepEqual(resp.Context, msg.Context) {
						t1.Errorf("input and output context are not equal")
					}
					if resp.Index != counter {
						t1.Errorf("index: got %d, want %d", resp.Index, counter)
					}
					if resp.Total != length {
						t1.Errorf("total: got %d, want %d", resp.Total, length)
					}
					counter++
					if resp.Item == 1 || resp.Item == 2 || resp.Item == 5 {
						//all good
						return module.Result{}
					}
					t1.Errorf("unexpected split output: %v", data)
					return module.Result{}
				}
				handle(context.Background(), resp, "", msg)
				return module.Result{}
			},
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &Component{}
			r := tt.handleFunc(t1, t.Handle)
			if (r.Err() != nil) != tt.wantErr {
				t1.Errorf("Handle() error = %v, wantErr %v", r.Err(), tt.wantErr)
			}
		})
	}
}
