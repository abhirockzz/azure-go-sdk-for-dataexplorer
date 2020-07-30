package cli

import (
	"log"

	"github.com/spf13/cobra"
)

const (
	subscriptionFlag  = "sub"
	resourceGroupFlag = "rg"
	nameFlag          = "name"
	locationFlag      = "loc"
)

var rootCmd = cobra.Command{Use: "goadx", Short: "CLI to test sample program for Azure Data Explorer"}

// Init serves as entrypoint for CLI
func Init() {
	err := rootCmd.Execute()
	if err != nil {
		log.Fatal("error CLI init ", err)
	}
}
