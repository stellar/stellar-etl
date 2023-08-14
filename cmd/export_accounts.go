package cmd

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/xdr"
)

var accountsCmd = &cobra.Command{
	Use:   "export_accounts",
	Short: "Exports the account data.",
	Long: `Exports historical account data from the genesis ledger to the provided end-ledger to an output file. 
The command reads from the bucket list, which includes the full history of the Stellar ledger. As a result, it 
should be used in an initial data dump. In order to get account information within a specified ledger range, see 
the export_ledger_entry_changes command.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		endNum, strictExport, isTest, extra := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = strictExport
		env := utils.GetEnvironmentDetails(isTest)
		path := utils.MustBucketFlags(cmd.Flags(), cmdLogger)
		gcsBucket, gcpCredentials := utils.MustGcsFlags(cmd.Flags(), cmdLogger)

		accounts, err := input.GetEntriesFromGenesis(endNum, xdr.LedgerEntryTypeAccount, env.ArchiveURLs)
		if err != nil {
			cmdLogger.Fatal("could not read accounts: ", err)
		}

		LedgerCloseMeta, err := env.GetLedgerCloseMeta(endNum)
		if err != nil {
			cmdLogger.Fatal("could not read ledger close meta: ", err)
		}

		outFile := mustOutFile(path)
		numFailures := 0
		totalNumBytes := 0
		for _, acc := range accounts {
			transformed, err := transform.TransformAccount(acc, LedgerCloseMeta)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not json transform account: %v", err))
				numFailures += 1
				continue
			}

			numBytes, err := exportEntry(transformed, outFile, extra)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not export entry: %v", err))
				numFailures += 1
				continue
			}
			totalNumBytes += numBytes
		}

		outFile.Close()
		cmdLogger.Info("Number of bytes written: ", totalNumBytes)

		printTransformStats(len(accounts), numFailures)

		maybeUpload(gcpCredentials, gcsBucket, path)
	},
}

func init() {
	rootCmd.AddCommand(accountsCmd)
	utils.AddCommonFlags(accountsCmd.Flags())
	utils.AddBucketFlags("accounts", accountsCmd.Flags())
	utils.AddGcsFlags(accountsCmd.Flags())
	accountsCmd.MarkFlagRequired("end-ledger")
	/*
		Current flags:
			end-ledger: the ledger sequence number for the end of the export range (required)
			output-file: filename of the output file

		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			 end time as a replacement for end sequence numbers
	*/
}
