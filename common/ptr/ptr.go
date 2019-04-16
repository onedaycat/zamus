package ptr

import (
    "time"
)

// String returns a pointer to the string value passed in.
//noinspection GoUnusedExportedFunction
func String(v string) *string {
    return &v
}

// StringValue returns the value of the string pointer passed in or
// "" if the pointer is nil.
//noinspection GoUnusedExportedFunction
//noinspection GoUnusedExportedFunction
func StringValue(v *string) string {
    if v != nil {
        return *v
    }
    return ""
}

// StringSlice converts a slice of string values into a slice of
// string pointers
//noinspection GoUnusedExportedFunction
func StringSlice(src []string) []*string {
    dst := make([]*string, len(src))
    for i := 0; i < len(src); i++ {
        dst[i] = &(src[i])
    }
    return dst
}

// StringValueSlice converts a slice of string pointers into a slice of
// string values
//noinspection GoUnusedExportedFunction
func StringValueSlice(src []*string) []string {
    dst := make([]string, len(src))
    for i := 0; i < len(src); i++ {
        if src[i] != nil {
            dst[i] = *(src[i])
        }
    }
    return dst
}

// StringMap converts a string map of string values into a string
// map of string pointers
//noinspection GoUnusedExportedFunction
func StringMap(src map[string]string) map[string]*string {
    dst := make(map[string]*string)
    for k, val := range src {
        v := val
        dst[k] = &v
    }
    return dst
}

// StringValueMap converts a string map of string pointers into a string
// map of string values
//noinspection GoUnusedExportedFunction
func StringValueMap(src map[string]*string) map[string]string {
    dst := make(map[string]string)
    for k, val := range src {
        if val != nil {
            dst[k] = *val
        }
    }
    return dst
}

// Bool returns a pointer to the bool value passed in.
//noinspection GoUnusedExportedFunction
func Bool(v bool) *bool {
    return &v
}

// BoolValue returns the value of the bool pointer passed in or
// false if the pointer is nil.
//noinspection GoUnusedExportedFunction
func BoolValue(v *bool) bool {
    if v != nil {
        return *v
    }
    return false
}

// BoolSlice converts a slice of bool values into a slice of
// bool pointers
//noinspection GoUnusedExportedFunction
func BoolSlice(src []bool) []*bool {
    dst := make([]*bool, len(src))
    for i := 0; i < len(src); i++ {
        dst[i] = &(src[i])
    }
    return dst
}

// BoolValueSlice converts a slice of bool pointers into a slice of
// bool values
//noinspection GoUnusedExportedFunction
func BoolValueSlice(src []*bool) []bool {
    dst := make([]bool, len(src))
    for i := 0; i < len(src); i++ {
        if src[i] != nil {
            dst[i] = *(src[i])
        }
    }
    return dst
}

// BoolMap converts a string map of bool values into a string
// map of bool pointers
//noinspection GoUnusedExportedFunction
func BoolMap(src map[string]bool) map[string]*bool {
    dst := make(map[string]*bool)
    for k, val := range src {
        v := val
        dst[k] = &v
    }
    return dst
}

// BoolValueMap converts a string map of bool pointers into a string
// map of bool values
//noinspection GoUnusedExportedFunction
func BoolValueMap(src map[string]*bool) map[string]bool {
    dst := make(map[string]bool)
    for k, val := range src {
        if val != nil {
            dst[k] = *val
        }
    }
    return dst
}

// Int returns a pointer to the int value passed in.
//noinspection GoUnusedExportedFunction
func Int(v int) *int {
    return &v
}

// IntValue returns the value of the int pointer passed in or
// 0 if the pointer is nil.
//noinspection GoUnusedExportedFunction
func IntValue(v *int) int {
    if v != nil {
        return *v
    }
    return 0
}

// IntSlice converts a slice of int values into a slice of
// int pointers
//noinspection GoUnusedExportedFunction
func IntSlice(src []int) []*int {
    dst := make([]*int, len(src))
    for i := 0; i < len(src); i++ {
        dst[i] = &(src[i])
    }
    return dst
}

// IntValueSlice converts a slice of int pointers into a slice of
// int values
//noinspection GoUnusedExportedFunction
func IntValueSlice(src []*int) []int {
    dst := make([]int, len(src))
    for i := 0; i < len(src); i++ {
        if src[i] != nil {
            dst[i] = *(src[i])
        }
    }
    return dst
}

// IntMap converts a string map of int values into a string
// map of int pointers
//noinspection GoUnusedExportedFunction
func IntMap(src map[string]int) map[string]*int {
    dst := make(map[string]*int)
    for k, val := range src {
        v := val
        dst[k] = &v
    }
    return dst
}

// IntValueMap converts a string map of int pointers into a string
// map of int values ,GoUnusedExportedFunction,GoUnusedExportedFunction,GoUnusedExportedFunction,GoUnusedExportedFunction,GoUnusedExportedFunction
//noinspection GoUnusedExportedFunction
func IntValueMap(src map[string]*int) map[string]int {
    dst := make(map[string]int)
    for k, val := range src {
        if val != nil {
            dst[k] = *val
        }
    }
    return dst
}

// Int64 returns a pointer to the int64 value passed in.
//noinspection GoUnusedExportedFunction
func Int64(v int64) *int64 {
    return &v
}

// Int64Value returns the value of the int64 pointer passed in or
// 0 if the pointer is nil.
//noinspection GoUnusedExportedFunction
func Int64Value(v *int64) int64 {
    if v != nil {
        return *v
    }
    return 0
}

// Int64Slice converts a slice of int64 values into a slice of
// int64 pointers
//noinspection GoUnusedExportedFunction
func Int64Slice(src []int64) []*int64 {
    dst := make([]*int64, len(src))
    for i := 0; i < len(src); i++ {
        dst[i] = &(src[i])
    }
    return dst
}

// Int64ValueSlice converts a slice of int64 pointers into a slice of
// int64 values
//noinspection GoUnusedExportedFunction
func Int64ValueSlice(src []*int64) []int64 {
    dst := make([]int64, len(src))
    for i := 0; i < len(src); i++ {
        if src[i] != nil {
            dst[i] = *(src[i])
        }
    }
    return dst
}

// Int64Map converts a string map of int64 values into a string
// map of int64 pointers
//noinspection GoUnusedExportedFunction
func Int64Map(src map[string]int64) map[string]*int64 {
    dst := make(map[string]*int64)
    for k, val := range src {
        v := val
        dst[k] = &v
    }
    return dst
}

// Int64ValueMap converts a string map of int64 pointers into a string
// map of int64 values
//noinspection GoUnusedExportedFunction
func Int64ValueMap(src map[string]*int64) map[string]int64 {
    dst := make(map[string]int64)
    for k, val := range src {
        if val != nil {
            dst[k] = *val
        }
    }
    return dst
}

// Float64 returns a pointer to the float64 value passed in.
//noinspection GoUnusedExportedFunction
func Float64(v float64) *float64 {
    return &v
}

// Float64Value returns the value of the float64 pointer passed in or
// 0 if the pointer is nil.
//noinspection GoUnusedExportedFunction
func Float64Value(v *float64) float64 {
    if v != nil {
        return *v
    }
    return 0
}

// Float64Slice converts a slice of float64 values into a slice of
// float64 pointers
//noinspection GoUnusedExportedFunction
func Float64Slice(src []float64) []*float64 {
    dst := make([]*float64, len(src))
    for i := 0; i < len(src); i++ {
        dst[i] = &(src[i])
    }
    return dst
}

// Float64ValueSlice converts a slice of float64 pointers into a slice of
// float64 values
//noinspection GoUnusedExportedFunction
func Float64ValueSlice(src []*float64) []float64 {
    dst := make([]float64, len(src))
    for i := 0; i < len(src); i++ {
        if src[i] != nil {
            dst[i] = *(src[i])
        }
    }
    return dst
}

// Float64Map converts a string map of float64 values into a string
// map of float64 pointers
//noinspection GoUnusedExportedFunction
func Float64Map(src map[string]float64) map[string]*float64 {
    dst := make(map[string]*float64)
    for k, val := range src {
        v := val
        dst[k] = &v
    }
    return dst
}

// Float64ValueMap converts a string map of float64 pointers into a string
// map of float64 values
//noinspection GoUnusedExportedFunction
func Float64ValueMap(src map[string]*float64) map[string]float64 {
    dst := make(map[string]float64)
    for k, val := range src {
        if val != nil {
            dst[k] = *val
        }
    }
    return dst
}

// Time returns a pointer to the time.Time value passed in.
//noinspection GoUnusedExportedFunction
func Time(v time.Time) *time.Time {
    return &v
}

// TimeValue returns the value of the time.Time pointer passed in or
// time.Time{} if the pointer is nil.
//noinspection GoUnusedExportedFunction
func TimeValue(v *time.Time) time.Time {
    if v != nil {
        return *v
    }
    return time.Time{}
}

// SecondsTimeValue converts an int64 pointer to a time.Time value
// representing seconds since Epoch or time.Time{} if the pointer is nil.
//noinspection GoUnusedExportedFunction
func SecondsTimeValue(v *int64) time.Time {
    if v != nil {
        return time.Unix(*v/1000, 0)
    }
    return time.Time{}
}

// MillisecondsTimeValue converts an int64 pointer to a time.Time value
// representing milliseconds sinch Epoch or time.Time{} if the pointer is nil.
//noinspection GoUnusedExportedFunction
func MillisecondsTimeValue(v *int64) time.Time {
    if v != nil {
        return time.Unix(0, *v*1000000)
    }
    return time.Time{}
}

// TimeUnixMilli returns a Unix timestamp in milliseconds from "January 1, 1970 UTC".
// The result is undefined if the Unix time cannot be represented by an int64.
// Which includes calling TimeUnixMilli on a zero Time is undefined.
//
// This utility is useful for service API's such as CloudWatch Logs which require
// their unix time values to be in milliseconds.
//
// See Go stdlib https://golang.org/pkg/time/#Time.UnixNano for more information.
//noinspection GoUnusedExportedFunction
func TimeUnixMilli(t time.Time) int64 {
    return t.UnixNano() / int64(time.Millisecond/time.Nanosecond)
}

// TimeSlice converts a slice of time.Time values into a slice of
// time.Time pointers
//noinspection GoUnusedExportedFunction
func TimeSlice(src []time.Time) []*time.Time {
    dst := make([]*time.Time, len(src))
    for i := 0; i < len(src); i++ {
        dst[i] = &(src[i])
    }
    return dst
}

// TimeValueSlice converts a slice of time.Time pointers into a slice of
// time.Time values
//noinspection GoUnusedExportedFunction
func TimeValueSlice(src []*time.Time) []time.Time {
    dst := make([]time.Time, len(src))
    for i := 0; i < len(src); i++ {
        if src[i] != nil {
            dst[i] = *(src[i])
        }
    }
    return dst
}

// TimeMap converts a string map of time.Time values into a string
// map of time.Time pointers
//noinspection GoUnusedExportedFunction
func TimeMap(src map[string]time.Time) map[string]*time.Time {
    dst := make(map[string]*time.Time)
    for k, val := range src {
        v := val
        dst[k] = &v
    }
    return dst
}

// TimeValueMap converts a string map of time.Time pointers into a string
// map of time.Time values
//noinspection GoUnusedExportedFunction
func TimeValueMap(src map[string]*time.Time) map[string]time.Time {
    dst := make(map[string]time.Time)
    for k, val := range src {
        if val != nil {
            dst[k] = *val
        }
    }
    return dst
}
