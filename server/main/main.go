package main

import (
	"server/logging"
	"server/services"

	log "github.com/sirupsen/logrus"
)

func main() {

	logging.SettingLogger(log.InfoLevel, "server")

	rsaPrivateKeyPath := "pv.pem"
	rsaPublicteKeyPath := "pub.pem"
	network := "tcp"

	services.Start(rsaPrivateKeyPath, rsaPublicteKeyPath, network)

	for {

	}
}
