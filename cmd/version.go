package cmd

import (
	"fmt"
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
			fmt.Printf("stellar-etl (unknown)\n")
			return
		}
		fmt.Printf("stellar-etl %s\n", buildInfo.Main.Version)

		// Find and display versions of libs containing XDR
		printDepVersion(buildInfo, "github.com/stellar/go")
		printDepVersion(buildInfo, "github.com/stellar/go-stellar-xdr-json")
	},
}

func printDepVersion(buildInfo *debug.BuildInfo, name string) {
	version := "unknown"
	for _, dep := range buildInfo.Deps {
		if dep.Path == name {
			version = dep.Version
			break
		}
	}
	fmt.Printf("%s %s\n", name, version)
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
