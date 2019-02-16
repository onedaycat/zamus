package gocqrs

import (
	"github.com/onedaycat/errors"
)

var ErrNotFound = errors.NotFound("es1", "Not Found")
var ErrVersionInconsistency = errors.BadRequest("es2", "Version is inconsistency")
var ErrEncodingNotSupported = errors.InternalError("es3", "Unable unmarshal payload, unsupport encoding")
var ErrEventLimitExceed = errors.InternalError("es4", "Number of events in aggregate limit exceed")
var ErrInvalidVersionNotAllowed = errors.InternalError("es5", "Event sequence should not be 0")
var ErrNoAggregateID = errors.InternalError("es6", "No aggregate id in aggregate root")
