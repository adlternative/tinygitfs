/*
Copyright © 2023 ZheNing Hu <adlternative@gmail.com>
*/
package cmd

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "tinygitfs",
	Short: "git file system",
	Long:  `A minimalist git file system build on cloud`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		log.WithError(err).Fatal("gitfs execute failed")
	}
}

var loglevel string

func init() {
	cobra.OnInitialize(initLog)

	rootCmd.PersistentFlags().StringVar(&loglevel, "loglevel", "info", "log level")
}

func initLog() {
	lvl, err := log.ParseLevel(loglevel)
	if err != nil {
		panic(err)
	}
	log.SetLevel(lvl)
}
