package server

import (
	"fmt"

	"github.com/klaudworks/klite/internal/sasl"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Handler processes a Kafka request and returns a response.
// Returning (nil, nil) means no response should be sent (e.g., acks=0 produce).
// Returning (nil, err) closes the connection.
type Handler func(req kmsg.Request) (kmsg.Response, error)

// ConnContext provides per-connection state to handlers that need it (e.g. SASL).
type ConnContext struct {
	SASLStage *SASLStage
	ScramS0   **sasl.ScramServer0
	User      *string
}

// ConnHandler processes a request with access to per-connection state.
// Used for SASL handlers that need to modify connection-level auth state.
type ConnHandler func(req kmsg.Request, cc ConnContext) (kmsg.Response, error)

// HandlerRegistry maps API keys to handler functions.
type HandlerRegistry struct {
	handlers     map[int16]Handler
	connHandlers map[int16]ConnHandler
}

// NewHandlerRegistry creates an empty handler registry.
func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{
		handlers:     make(map[int16]Handler),
		connHandlers: make(map[int16]ConnHandler),
	}
}

// Register registers a handler for the given API key.
func (r *HandlerRegistry) Register(key int16, h Handler) {
	r.handlers[key] = h
}

// RegisterConn registers a connection-aware handler for the given API key.
func (r *HandlerRegistry) RegisterConn(key int16, h ConnHandler) {
	r.connHandlers[key] = h
}

// Get returns the handler for the given API key, or nil if not registered.
func (r *HandlerRegistry) Get(key int16) Handler {
	return r.handlers[key]
}

// GetConn returns the connection-aware handler for the given API key.
func (r *HandlerRegistry) GetConn(key int16) ConnHandler {
	return r.connHandlers[key]
}

// dispatch routes a request to the appropriate handler.
// Returns the response and any error. Unknown API keys are handled
// at the connection level (close connection) before reaching dispatch.
func (cc *clientConn) dispatchReq(kreq kmsg.Request) (kmsg.Response, error) {
	key := kreq.Key()

	// Check for connection-aware handler first (SASL handlers)
	connHandler := cc.server.handlers.GetConn(key)
	if connHandler != nil {
		ctx := ConnContext{
			SASLStage: &cc.saslStage,
			ScramS0:   &cc.scramS0,
			User:      &cc.user,
		}
		return connHandler(kreq, ctx)
	}

	handler := cc.server.handlers.Get(key)
	if handler == nil {
		return nil, fmt.Errorf("no handler for API key %d", key)
	}
	return handler(kreq)
}
