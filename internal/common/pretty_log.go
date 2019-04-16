package common

import (
	"fmt"
	"os"
	"strings"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func PrettyLog() {
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
			return strings.ToUpper(fmt.Sprintf("LEVEL=%s", i))
		},
		FormatMessage: func(i interface{}) string {
			return fmt.Sprintf(`MSG="%s"`, i)
		},
	})
}
