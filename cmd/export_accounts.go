package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"
)

var accountsCmd = &cobra.Command{
	Use:   "export_accounts",
	Short: "Exports the account data.",
	Long:  `Exports historical account data within the specified range to an output file.`,
	Run: func(cmd *cobra.Command, args []string) {
		startNum, endNum, limit, path, useStdout := utils.MustBasicFlags(cmd.Flags(), cmdLogger)

		var outFile *os.File
		if !useStdout {
			outFile = mustOutFile(path)
		}

		accounts, err := input.GetAccounts(startNum, endNum, limit)
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
