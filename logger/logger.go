package logger

type Level int

const (
    DebugLevel Level = iota
    InfoLevel
    WarnLevel
    ErrorLevel
    PanicLevel
)

type Logger interface {
    SetLevel(level Level)
    Pretty(pretty bool)
    Debug(msg string, data ...interface{})
    Info(msg string, data ...interface{})
    Warn(msg string, data ...interface{})
    Error(err error, data ...interface{})
    Panic(err error, data ...interface{})
}
