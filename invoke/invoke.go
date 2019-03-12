package invoke

import (
	"encoding/json"

	jsoniter "github.com/json-iterator/go"
	"github.com/onedaycat/errors"
	appErr "github.com/onedaycat/zamus/errors"
)

type InvokeRequest struct {
	Function      string      `json:"function"`
	Args          interface{} `json:"arguments,omitempty"`
	Source        interface{} `json:"source,omitempty"`
	Identity      *Identity   `json:"identity,omitempty"`
	PermissionKey string      `json:"pemKey,omitempty"`
}

func (r *InvokeRequest) MarshalRequest() ([]byte, errors.Error) {
	data, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(r)
	if err != nil {
		return nil, appErr.ErrUnableUnmarshal.WithCause(err).WithCaller().WithInput(r)
	}

	return data, nil
}

type InvokeEvent struct {
	Function      string          `json:"function"`
	Args          json.RawMessage `json:"arguments,omitempty"`
	Source        json.RawMessage `json:"source,omitempty"`
	Identity      *Identity       `json:"identity,omitempty"`
	PermissionKey string          `json:"pemKey,omitempty"`
	Warmer        bool            `json:"warmer,omitempty"`
	Concurency    int             `json:"concurency,omitempty"`
	CorrelationID string          `json:"correlationID,omitempty"`
}

func (e *InvokeEvent) ParseArgs(v interface{}) errors.Error {
	if err := jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(e.Args, v); err != nil {
		return appErr.ErrUnableUnmarshal.WithCause(err).WithCaller()
	}

	return nil
}

func (e *InvokeEvent) ParseSource(v interface{}) errors.Error {
	if err := jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(e.Source, v); err != nil {
		return appErr.ErrUnableUnmarshal.WithCause(err).WithCaller()
	}

	return nil
}
