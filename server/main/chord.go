package main

import (
	"server/chord"
	"server/logging"
)

func main() {

	logging.SettingLogger("chord")

	chord.Test()
}
