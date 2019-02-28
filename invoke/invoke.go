package invoke

import (
	"encoding/json"
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

func (e *InvokeEvent) ParseArgs(v interface{}) error {
	return json.Unmarshal(e.Args, v)
}

func (e *InvokeEvent) ParseSource(v interface{}) error {
	return json.Unmarshal(e.Source, v)
}
