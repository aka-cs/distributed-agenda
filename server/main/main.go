package main

import (
	"server/services"

	log "github.com/sirupsen/logrus"
	easy "github.com/t-tomalak/logrus-easy-formatter"
)

func main() {

	log.SetLevel(log.InfoLevel)
	log.SetFormatter(&easy.Formatter{
		TimestampFormat: "15:04:05",
		LogFormat:       "[%lvl%]: %time% - %msg%",
	})

	rsaKeyPath := "pv.pem"
	network := "tcp"

	go services.StartGroupService(network, "0.0.0.0:50052")
	go services.StartEventService(network, "0.0.0.0:50053")
	go services.StartAuthServer(rsaKeyPath, network, "0.0.0.0:50054")
	go services.StartHistoryService(network, "0.0.0.0:50055")
	services.StartUserService(network, "0.0.0.0:50051")
}
