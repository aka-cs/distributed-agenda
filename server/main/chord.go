package main

import (
	log "github.com/sirupsen/logrus"
	easy "github.com/t-tomalak/logrus-easy-formatter"
	"server/chord"
)

func main() {
	log.SetLevel(log.DebugLevel)
	log.SetFormatter(&easy.Formatter{
		TimestampFormat: "15:04:05",
		LogFormat:       "[%lvl%]: %time% - %msg%",
	})

	chord.Test()
}
