package socketio

import (
	"encoding/json"
	"reflect"
)

// invokeHandler calls a registered handler with the socket and the decoded
// event arguments. The first handler parameter must be *Socket. Return values
// are collected as the acknowledgement payload.
//
// Supported signatures:
//
//	func(s *Socket)
//	func(s *Socket, a string)
//	func(s *Socket, a int, b map[string]any)
//	func(s *Socket, a *T)
//	... with zero or more return values used as the ack payload.
func invokeHandler(h reflect.Value, s *Socket, args []any) []any {
	ht := h.Type()
	numIn := ht.NumIn()
	in := make([]reflect.Value, numIn)
	for i := 0; i < numIn; i++ {
		if i == 0 {
			if !ht.In(0).AssignableTo(reflect.TypeOf(s)) {
				return nil
			}
			in[i] = reflect.ValueOf(s)
			continue
		}
		pt := ht.In(i)
		argIndex := i - 1
		if argIndex < len(args) {
			v, ok := convertArg(args[argIndex], pt)
			if !ok {
				return nil
			}
			in[i] = v
		} else {
			in[i] = reflect.Zero(pt)
		}
	}

	outs := h.Call(in)
	return collectResults(outs)
}

// invokeClientHandler calls a client-side event handler with the decoded event
// arguments. Unlike invokeHandler there is no *Socket first parameter, so the
// first handler parameter maps to the first event argument. Return values are
// collected as the acknowledgement payload. A non-nil error is returned when an
// argument cannot be decoded into the handler's parameter type (wrapping
// ErrHandlerMismatch); the caller is responsible for surfacing it.
//
// Supported signatures:
//
//	func()
//	func(a string)
//	func(a int, b map[string]any)
//	func(a *T)
//	... with zero or more return values used as the ack payload.
func invokeClientHandler(h reflect.Value, args []any) ([]any, error) {
	ht := h.Type()
	numIn := ht.NumIn()
	in := make([]reflect.Value, numIn)
	for i := 0; i < numIn; i++ {
		if i < len(args) {
			v, ok := convertArg(args[i], ht.In(i))
			if !ok {
				return nil, ErrHandlerMismatch
			}
			in[i] = v
		} else {
			in[i] = reflect.Zero(ht.In(i))
		}
	}
	outs := h.Call(in)
	return collectResults(outs), nil
}

// collectResults flattens a handler's return values into an acknowledgement
// payload, skipping nil interfaces, nil errors and treating non-nil errors as
// their message string.
func collectResults(outs []reflect.Value) []any {
	var result []any
	for _, o := range outs {
		if !o.IsValid() {
			continue
		}
		if o.Kind() == reflect.Interface && o.IsNil() {
			continue
		}
		if err, ok := o.Interface().(error); ok {
			if err == nil {
				continue
			}
			result = append(result, err.Error())
			continue
		}
		result = append(result, o.Interface())
	}
	return result
}

// convertArg converts a decoded JSON value into the handler parameter type via
// a JSON round trip. Container types receive the decoded value as-is so that
// nested []byte values survive the dispatch unchanged.
func convertArg(v any, t reflect.Type) (reflect.Value, bool) {
	if t == reflect.TypeOf((*any)(nil)).Elem() {
		return reflect.ValueOf(v), true
	}
	switch t {
	case reflect.TypeOf(map[string]any(nil)), reflect.TypeOf([]any(nil)):
		return reflect.ValueOf(v), true
	}
	b, err := json.Marshal(v)
	if err != nil {
		return reflect.Value{}, false
	}
	out := reflect.New(t)
	if err := json.Unmarshal(b, out.Interface()); err != nil {
		return reflect.Value{}, false
	}
	return out.Elem(), true
}
