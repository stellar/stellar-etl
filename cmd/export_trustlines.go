package cmd

import (
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/xdr"
)

// trustlinesCmd represents the trustlines command
var trustlinesCmd = &cobra.Command{
	Use:   "export_trustlines",
	Short: "Exports the trustline data over a specified range.",
	Long: `Exports historical trustline data from the genesis ledger to the provided end-ledger to an output file. 
	The command reads from the bucket list, which includes the full history of the Stellar ledger. As a result, it 
	should be used in an initial data dump. In order to get trustline information within a specified ledger range, see 
	the export_ledger_entry_changes command.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		endNum, strictExport, isTest, isFuture, extra := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = strictExport
		env := utils.GetEnvironmentDetails(isTest, isFuture)
		path := utils.MustBucketFlags(cmd.Flags(), cmdLogger)
		gcsBucket, gcpCredentials := utils.MustGcsFlags(cmd.Flags(), cmdLogger)

		trustlines, err := input.GetEntriesFromGenesis(endNum, xdr.LedgerEntryTypeTrustline, env.ArchiveURLs)
		if err != nil {
			cmdLogger.Fatal("could not read trustlines: ", err)
		}

		outFile := mustOutFile(path)
		numFailures := 0
		totalNumBytes := 0
		for _, trust := range trustlines {
			var closedAt time.Time
			transformed, err := transform.TransformTrustline(trust, closedAt)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not json transform trustline %+v: %v", trust, err))
				numFailures += 1
				continue
			}

			numBytes, err := exportEntry(transformed, outFile, extra)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not export trustline %+v: %v", trust, err))
				numFailures += 1
				continue
			}
			totalNumBytes += numBytes
		}

		outFile.Close()

		cmdLogger.Info("Number of bytes written: ", totalNumBytes)

		printTransformStats(len(trustlines), numFailures)

		maybeUpload(gcpCredentials, gcsBucket, path)
	},
}

func init() {
	rootCmd.AddCommand(trustlinesCmd)
	utils.AddCommonFlags(trustlinesCmd.Flags())
	utils.AddBucketFlags("trustlines", trustlinesCmd.Flags())
	utils.AddGcsFlags(trustlinesCmd.Flags())
	trustlinesCmd.MarkFlagRequired("end-ledger")

	/*
		Current flags:
			end-ledger: the ledger sequence number for the end of the export range (required)
			output-file: filename of the output file
			stdout: if set, output is printed to stdout

		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			 end time as a replacement for end sequence numbers
	*/
}
