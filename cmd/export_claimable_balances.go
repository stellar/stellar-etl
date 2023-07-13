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

var claimableBalancesCmd = &cobra.Command{
	Use:   "export_claimable_balances",
	Short: "Exports the data on claimable balances made from the genesis ledger to a specified endpoint.",
	Long: `Exports historical offer data from the genesis ledger to the provided end-ledger to an output file. 
	The command reads from the bucket list, which includes the full history of the Stellar ledger. As a result, it 
	should be used in an initial data dump. In order to get offer information within a specified ledger range, see 
	the export_ledger_entry_changes command.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		endNum, strictExport, isTest, isFuture, extra := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = strictExport
		env := utils.GetEnvironmentDetails(isTest, isFuture)
		path := utils.MustBucketFlags(cmd.Flags(), cmdLogger)
		gcsBucket, gcpCredentials := utils.MustGcsFlags(cmd.Flags(), cmdLogger)

		balances, err := input.GetEntriesFromGenesis(endNum, xdr.LedgerEntryTypeClaimableBalance, env.ArchiveURLs)
		if err != nil {
			cmdLogger.Fatal("could not read balances: ", err)
		}

		outFile := mustOutFile(path)
		numFailures := 0
		totalNumBytes := 0
		for _, balance := range balances {
			transformed, err := transform.TransformClaimableBalance(balance)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not transform balance %+v: %v", balance, err))
				numFailures += 1
				continue
			}

			numBytes, err := exportEntry(transformed, outFile, extra)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not export balance %+v: %v", balance, err))
				numFailures += 1
				continue
			}
			totalNumBytes += numBytes
		}

		outFile.Close()
		cmdLogger.Info("Number of bytes written: ", totalNumBytes)

		printTransformStats(len(balances), numFailures)

		maybeUpload(gcpCredentials, gcsBucket, path)
	},
}

func init() {
	rootCmd.AddCommand(claimableBalancesCmd)
	utils.AddCommonFlags(claimableBalancesCmd.Flags())
	utils.AddBucketFlags("claimable_balances", claimableBalancesCmd.Flags())
	utils.AddGcsFlags(claimableBalancesCmd.Flags())
	claimableBalancesCmd.MarkFlagRequired("end-ledger")

    /*
		Current flags:
			end-ledger: the ledger sequence number for the end of the export range (required)
            output-file: filename of the output file

		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			end time as a replacement for end sequence numbers
    */
}
