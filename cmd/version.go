package cmd

import (
	"fmt"
	"io"
	"runtime/debug"

	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Display version information",
	Long:  `Display the version of stellar-etl and the versions of XDR libs.`,
	Run: func(cmd *cobra.Command, args []string) {
		buildInfo, ok := debug.ReadBuildInfo()
		if !ok {
			fmt.Fprintf(cmd.OutOrStdout(), "stellar-etl (unknown)\n")
			return
		}
		fmt.Fprintf(cmd.OutOrStdout(), "stellar-etl %s\n", buildInfo.Main.Version)

		// Find and display versions of libs containing XDR
		printDepVersion(cmd.OutOrStdout(), buildInfo, "github.com/stellar/go-stellar-sdk")
		printDepVersion(cmd.OutOrStdout(), buildInfo, "github.com/stellar/go-stellar-sdk-stellar-xdr-json")
	},
}

func printDepVersion(out io.Writer, buildInfo *debug.BuildInfo, name string) {
	version := "(unknown)"
	for _, dep := range buildInfo.Deps {
		if dep.Path == name {
			version = dep.Version
			break
		}
	}
	fmt.Fprintf(out, "%s %s\n", name, version)
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
