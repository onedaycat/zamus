package invoke

import (
	"encoding/json"

	"github.com/onedaycat/zamus/errors"
)

type InvokeRequest struct {
	Function      string      `json:"function"`
	Args          interface{} `json:"arguments,omitempty"`
	Source        interface{} `json:"source,omitempty"`
	Identity      *Identity   `json:"identity,omitempty"`
	PermissionKey string      `json:"pemKey,omitempty"`
}

type InvokeEvent struct {
	Function      string          `json:"function"`
	Args          json.RawMessage `json:"arguments,omitempty"`
	Source        json.RawMessage `json:"source,omitempty"`
	Identity      *Identity       `json:"identity,omitempty"`
	PermissionKey string          `json:"pemKey,omitempty"`
}

func (e *InvokeEvent) ParseArgs(v interface{}) errors.Error {
	if err := json.Unmarshal(e.Args, v); err != nil {
		return errors.ErrUnableUnmarshal.WithCause(err).WithCaller()
	}

	return nil
}

func (e *InvokeEvent) ParseSource(v interface{}) errors.Error {
	if err := json.Unmarshal(e.Source, v); err != nil {
		return errors.ErrUnableUnmarshal.WithCause(err).WithCaller()
	}

	return nil
}
