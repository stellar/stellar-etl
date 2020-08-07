package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"
)

var accountsCmd = &cobra.Command{
	Use:   "export_accounts",
	Short: "Exports the account data.",
	Long: `Exports historical account data within the specified range to an output file.
			If the starting ledger is 1, the bucket list is used to acquire account data. Otherwise,
			a captive stellar core instance is used. In this case, the --core-executable and 
			--core-config flags are required. The stellar-core instance must be version 13.2.0 or greater`,
	Run: func(cmd *cobra.Command, args []string) {
		startNum, endNum, limit, path, useStdout := utils.MustBasicFlags(cmd.Flags(), cmdLogger)

		// If not starting at the genesis ledger, the core flags are mandatory
		var execPath, configPath string
		if startNum != 1 {
			execPath, configPath = utils.MustCoreFlags(cmd.Flags(), cmdLogger)
			if execPath == "" {
				cmdLogger.Fatal("A path to the stellar-core executable is mandatory when not starting at the genesis ledger (ledger 1)")
			}

			if configPath == "" {
				cmdLogger.Fatal("A path to a config file for stellar-core is mandatory when not starting at the genesis ledger (ledger 1)")
			}

			var err error
			execPath, err = filepath.Abs(execPath)
			if err != nil {
				cmdLogger.Fatal("could not get absolute filepath for stellar-core executable: ", err)
			}

			configPath, err = filepath.Abs(configPath)
			if err != nil {
				cmdLogger.Fatal("could not get absolute filepath for the config file: ", err)
			}
		}

		var outFile *os.File
		if !useStdout {
			outFile = mustOutFile(path)
		}

		accounts, err := input.GetAccounts(startNum, endNum, limit, execPath, configPath)
		if err != nil {
			cmdLogger.Fatal("could not read accounts: ", err)
		}

		for _, acc := range accounts {
			transformed, err := transform.TransformAccount(acc)
			if err != nil {
				cmdLogger.Fatal("could not transform account", err)
			}

			marshalled, err := json.Marshal(transformed)
			if err != nil {
				cmdLogger.Fatal("could not json encode account", err)
			}

			if !useStdout {
				outFile.Write(marshalled)
				outFile.WriteString("\n")
			} else {
				fmt.Println(string(marshalled))
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(accountsCmd)
	utils.AddBasicFlags("accounts", accountsCmd.Flags())
	utils.AddCoreFlags(accountsCmd.Flags())
	accountsCmd.MarkFlagRequired("end-ledger")
	/*
		Current flags:
			start-ledger: the ledger sequence number for the beginning of the export period
			end-ledger: the ledger sequence number for the end of the export range (required)

			limit: maximum number of accounts to export
			output-file: filename of the output file

		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			start and end time as a replacement for start and end sequence numbers
	*/
}
