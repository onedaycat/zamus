package invoke

import (
	"encoding/json"
)

type InvokeRequest struct {
	Function      string      `json:"function"`
	Args          interface{} `json:"arguments"`
	Source        interface{} `json:"source"`
	Identity      *Identity   `json:"identity"`
	PermissionKey string      `json:"pemKey"`
}

type InvokeEvent struct {
	Function      string          `json:"function"`
	Args          json.RawMessage `json:"arguments"`
	Source        json.RawMessage `json:"source"`
	Identity      *Identity       `json:"identity"`
	PermissionKey string          `json:"pemKey"`
}

func (e *InvokeEvent) ParseArgs(v interface{}) error {
	return json.Unmarshal(e.Args, v)
}

func (e *InvokeEvent) ParseSource(v interface{}) error {
	return json.Unmarshal(e.Source, v)
}
