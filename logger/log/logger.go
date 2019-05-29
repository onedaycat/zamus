package log

import (
    "fmt"
    "os"
    "strings"

    "github.com/onedaycat/zamus/logger"
    "github.com/rs/zerolog"
    "github.com/rs/zerolog/log"
)

var Logger *zero

type zero struct{}

func (l *zero) Pretty(pretty bool) {
    if pretty {
        log.Logger = log.Output(zerolog.ConsoleWriter{
            Out:        os.Stderr,
            NoColor:    true,
            TimeFormat: "",
            PartsOrder: []string{
                zerolog.LevelFieldName,
                zerolog.CallerFieldName,
                zerolog.MessageFieldName,
            },
            FormatLevel: func(i interface{}) string {
                return strings.ToUpper(fmt.Sprintf("%s", i))
            },
            FormatMessage: func(i interface{}) string {
                return fmt.Sprintf(`MSG=%s`, i)
            },
        })
    } else {
        log.Logger = zerolog.New(os.Stderr).With().Timestamp().Logger()
    }
}

func (l *zero) SetLevel(level logger.Level) {
    switch level {
    case logger.DebugLevel:
        zerolog.SetGlobalLevel(zerolog.DebugLevel)
    case logger.InfoLevel:
        zerolog.SetGlobalLevel(zerolog.InfoLevel)
    case logger.WarnLevel:
        zerolog.SetGlobalLevel(zerolog.WarnLevel)
    case logger.ErrorLevel:
        zerolog.SetGlobalLevel(zerolog.ErrorLevel)
    case logger.PanicLevel:
        zerolog.SetGlobalLevel(zerolog.PanicLevel)
    }
}

func (l *zero) Debug(msg string, data ...interface{}) {
    ll := log.Debug()
    if len(data) > 0 {
        ll.Interface("data", data[0])
    }

    ll.Msg(msg)
}

func (l *zero) Info(msg string, data ...interface{}) {
    ll := log.Info()
    if len(data) > 0 {
        ll.Interface("data", data[0])
    }

    ll.Msg(msg)
}

func (l *zero) Warn(msg string, data ...interface{}) {
    ll := log.Warn()
    if len(data) > 0 {
        ll.Interface("data", data[0])
    }

    ll.Msg(msg)
}

func (l *zero) Error(err error, data ...interface{}) {
    if err == nil {
        return
    }

    ll := log.WithLevel(zerolog.ErrorLevel).Err(err)
    if len(data) > 0 {
        ll.Interface("data", data[0])
    }

    ll.Msg(err.Error())
}

func (l *zero) Panic(err error, data ...interface{}) {
    if err == nil {
        return
    }

    ll := log.WithLevel(zerolog.PanicLevel).Err(err)
    if len(data) > 0 {
        ll.Interface("data", data[0])
    }

    ll.Msg(err.Error())
}
