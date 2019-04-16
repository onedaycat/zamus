package common

import (
    "github.com/golang/snappy"
    jsoniter "github.com/json-iterator/go"
    "github.com/onedaycat/errors"
    appErr "github.com/onedaycat/zamus/errors"
)

var json = jsoniter.ConfigFastest

func MarshalJSON(v interface{}) ([]byte, errors.Error) {
    data, err := json.Marshal(v)
    if err != nil {
        return nil, appErr.ErrUnableMarshal.WithCause(err).WithCaller().WithInput(v)
    }

    return data, nil
}

func UnmarshalJSON(data []byte, v interface{}) errors.Error {
    if err := json.Unmarshal(data, v); err != nil {
        return appErr.ErrUnableUnmarshal.WithCause(err).WithCaller().WithInput(data)
    }

    return nil
}

func MarshalJSONSnappy(v interface{}) ([]byte, errors.Error) {
    data, err := json.Marshal(v)
    if err != nil {
        return nil, appErr.ErrUnableMarshal.WithCause(err).WithCaller().WithInput(v)
    }

    var dst []byte
    dst = snappy.Encode(dst, data)

    return dst, nil
}

func UnmarshalJSONSnappy(data []byte, v interface{}) errors.Error {
    var dst []byte
    var err error
    dst, err = snappy.Decode(dst, data)
    if err != nil {
        return appErr.ErrUnableDecode.WithCause(err).WithCaller().WithInput(data)
    }

    if err := json.Unmarshal(dst, v); err != nil {
        return appErr.ErrUnableUnmarshal.WithCause(err).WithCaller().WithInput(data)
    }

    return nil
}
