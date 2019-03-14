package command

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/common"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/invoke"
)

type CommandReq struct {
	Function      string              `json:"function"`
	Args          jsoniter.RawMessage `json:"arguments,omitempty"`
	Source        jsoniter.RawMessage `json:"source,omitempty"`
	Identity      *invoke.Identity    `json:"identity,omitempty"`
	PermissionKey string              `json:"pemKey,omitempty"`
	Warmer        bool                `json:"warmer,omitempty"`
	Concurency    int                 `json:"concurency,omitempty"`
	CorrelationID string              `json:"correlationID,omitempty"`
}

func NewCommandReq(fn string) *CommandReq {
	return &CommandReq{
		Function: fn,
	}
}

func (e *CommandReq) WithIdentity(id *Identity) *CommandReq {
	e.Identity = id

	return e
}

func (e *CommandReq) WithPermission(pem string) *CommandReq {
	e.PermissionKey = pem

	return e
}

func (e *CommandReq) WithArgs(args interface{}) *CommandReq {
	var err error
	e.Args, err = common.MarshalJSON(args)
	if err != nil {
		panic(appErr.ErrUnableUnmarshal.WithCause(err).WithCaller().WithInput(args))
	}

	return e
}

func (e *CommandReq) WithSource(source interface{}) *CommandReq {
	var err error
	e.Source, err = common.MarshalJSON(source)
	if err != nil {
		panic(appErr.ErrUnableUnmarshal.WithCause(err).WithCaller().WithInput(source))
	}

	return e
}

func (e *CommandReq) ParseArgs(v interface{}) errors.Error {
	if err := common.UnmarshalJSON(e.Args, v); err != nil {
		return appErr.ErrUnableUnmarshal.WithCause(err).WithCaller()
	}

	return nil
}

func (e *CommandReq) ParseSource(v interface{}) errors.Error {
	if err := common.UnmarshalJSON(e.Source, v); err != nil {
		return appErr.ErrUnableUnmarshal.WithCause(err).WithCaller()
	}

	return nil
}

func (e *CommandReq) MarshalRequest() ([]byte, errors.Error) {
	data, err := common.MarshalJSON(e)
	if err != nil {
		return nil, appErr.ErrUnableUnmarshal.WithCause(err).WithCaller().WithInput(e)
	}

	return data, nil
}
