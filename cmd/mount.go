/*
Copyright © 2023 ZheNing Hu <adlternative@gmail.com>
*/
package cmd

import (
	"context"
	"github.com/adlternative/tinygitfs/pkg/data"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"

	"github.com/adlternative/tinygitfs/pkg/gitfs"
	"github.com/spf13/cobra"
)

var (
	debug       bool
	metadataUrl string
	dataOption  data.Option
)

// mountCmd represents the mount command
var mountCmd = &cobra.Command{
	Use:   "mount",
	Short: "mount gitfs",
	Long:  `tinygitfs mount <dir>`,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		signals := []os.Signal{syscall.SIGTERM, syscall.SIGINT}
		termCh := make(chan os.Signal, len(signals))
		signal.Notify(termCh, signals...)

		server, err := gitfs.Mount(ctx, args[0], debug, metadataUrl, &dataOption)
		if err != nil {
			log.WithError(err).Errorf("gitfs mount failed")
			return
		}

		go func() {
		loop:
			for {
				// wait for done
				select {
				case <-ctx.Done():
				case s := <-termCh:
					log.Infof("received signal %q\n", s)
					err := server.Unmount()
					if err == nil {
						break loop
					}
					log.WithError(err).Errorf("gitfs unmount failed")
				}
			}
			cancel()
		}()

		server.Wait()
	},
}

func init() {
	rootCmd.AddCommand(mountCmd)

	mountCmd.Flags().BoolVar(&debug, "debug", false, "show fuse debug messages")
	mountCmd.Flags().StringVar(&metadataUrl, "metadata", "", "metadata url")
	mountCmd.Flags().StringVarP(&dataOption.EndPoint, "endpoint", "", "", "A endpoint URL to store data")
	mountCmd.Flags().StringVarP(&dataOption.Bucket, "bucket", "", "", "A bucket to store data")
	mountCmd.Flags().StringVarP(&dataOption.Accesskey, "access_key", "", "", "Access key for object storage (env ACCESS_KEY)")
	mountCmd.Flags().StringVarP(&dataOption.SecretKey, "secret_key", "", "", "Secret key for object storage  (env SECRET_KEY)")
}
