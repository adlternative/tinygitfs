/*
Copyright © 2023 ZheNing Hu <adlternative@gmail.com>
*/
package cmd

import (
	"context"

	"github.com/adlternative/tinygitfs/pkg/gitfs"
	"github.com/spf13/cobra"
)

var (
	debug       bool
	metadataUrl string
)

// mountCmd represents the mount command
var mountCmd = &cobra.Command{
	Use:   "mount",
	Short: "mount gitfs",
	Long:  `tinygitfs mount <dir>`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		return gitfs.Mount(ctx, args[0], debug, metadataUrl)
	},
}

func init() {
	rootCmd.AddCommand(mountCmd)

	mountCmd.Flags().BoolVar(&debug, "debug", false, "show fuse debug messages")
	mountCmd.Flags().StringVar(&metadataUrl, "metadata", "", "metadata url")
}
