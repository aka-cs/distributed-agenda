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

	/*
		//rsaKeyPath := ""
		network := "tcp"

		go services.StartGroupService(network, "0.0.0.0:50052")
		go services.StartEventService(network, "0.0.0.0:50053")
		//go services.StartAuthServer(rsaKeyPath, network, "0.0.0.0:50054")
		services.StartUserService(network, "0.0.0.0:50051")
	*/

	chord.Test()
}
