package reactor

import (
    "github.com/gogo/protobuf/proto"
)

//noinspection GoUnusedExportedFunction
func EventType(evt proto.Message) string {
    return proto.MessageName(evt)
}

//noinspection GoUnusedExportedFunction
func EventTypes(evts ...proto.Message) []string {
    types := make([]string, len(evts))
    for i, evt := range evts {
        types[i] = proto.MessageName(evt)
    }

    return types
}
