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

var accountSignersCmd = &cobra.Command{
	Use:   "export_signers",
	Short: "Exports the account signers data.",
	Long: `Exports historical account signers data from the genesis ledger to the provided end-ledger to an output file. 
The command reads from the bucket list, which includes the full history of the Stellar ledger. As a result, it 
should be used in an initial data dump. In order to get account information within a specified ledger range, see 
the export_ledger_entry_changes command.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		endNum, strictExport, isTest, isFuture, extra, _, datastoreUrl := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = strictExport
		env := utils.GetEnvironmentDetails(isTest, isFuture, datastoreUrl)
		path := utils.MustBucketFlags(cmd.Flags(), cmdLogger)
		cloudStorageBucket, cloudCredentials, cloudProvider := utils.MustCloudStorageFlags(cmd.Flags(), cmdLogger)

		accounts, err := input.GetEntriesFromGenesis(endNum, xdr.LedgerEntryTypeAccount, env.ArchiveURLs)
		if err != nil {
			cmdLogger.Fatal("could not read accounts: ", err)
		}

		outFile := mustOutFile(path)
		numFailures := 0
		totalNumBytes := 0
		numSigners := 0
		var header xdr.LedgerHeaderHistoryEntry
		for _, acc := range accounts {
			if acc.AccountSignersChanged() {
				transformed, err := transform.TransformSigners(acc, header)
				if err != nil {
					cmdLogger.LogError(fmt.Errorf("could not json transform account signer: %v", err))
					numFailures += 1
					continue
				}

				for _, entry := range transformed {
					numBytes, err := exportEntry(entry, outFile, extra)
					if err != nil {
						cmdLogger.LogError(fmt.Errorf("could not export entry: %v", err))
						numFailures += 1
						continue
					}
					numSigners += 1
					totalNumBytes += numBytes
				}
			}
		}

		outFile.Close()
		cmdLogger.Info("Number of bytes written: ", totalNumBytes)

		printTransformStats(numSigners, numFailures)

		maybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path)
	},
}

func init() {
	rootCmd.AddCommand(accountSignersCmd)
	utils.AddCommonFlags(accountSignersCmd.Flags())
	utils.AddBucketFlags("signers", accountSignersCmd.Flags())
	utils.AddCloudStorageFlags(accountSignersCmd.Flags())
	accountSignersCmd.MarkFlagRequired("end-ledger")
}
