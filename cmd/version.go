package cmd

import (
	"fmt"
	"runtime/debug"

	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Display version information",
	Long:  `Display the version of stellar-etl and the version of XDR supported.`,
	Run: func(cmd *cobra.Command, args []string) {
		// Get build info using runtime/debug
		buildInfo, ok := debug.ReadBuildInfo()
		if !ok {
			fmt.Println("Version information not available")
			return
		}

		// Display main module version
		fmt.Printf("stellar-etl version: %s\n", buildInfo.Main.Version)

		// Find and display stellar/go version (which provides XDR support)
		var stellarGoVersion string
		for _, dep := range buildInfo.Deps {
			if dep.Path == "github.com/stellar/go" {
				stellarGoVersion = dep.Version
				break
			}
		}

		if stellarGoVersion != "" {
			fmt.Printf("Stellar XDR version (github.com/stellar/go): %s\n", stellarGoVersion)
		} else {
			fmt.Println("Stellar XDR version: not found")
		}
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
