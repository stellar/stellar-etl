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

var expirationCmd = &cobra.Command{
	Use:   "export_expiration",
	Short: "Exports the expiration information.",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		endNum, strictExport, isTest, isFuture, extra := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = strictExport
		env := utils.GetEnvironmentDetails(isTest, isFuture)
		path := utils.MustBucketFlags(cmd.Flags(), cmdLogger)
		gcsBucket, gcpCredentials := utils.MustGcsFlags(cmd.Flags(), cmdLogger)

		expirations, err := input.GetEntriesFromGenesis(endNum, xdr.LedgerEntryTypeExpiration, env.ArchiveURLs)
		if err != nil {
			cmdLogger.Fatal("Error getting ledger entries: ", err)
		}

		outFile := mustOutFile(path)
		numFailures := 0
		totalNumBytes := 0
		for _, expiration := range expirations {
			var header xdr.LedgerHeaderHistoryEntry
			transformed, err := transform.TransformExpiration(expiration, header)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not transform expiration %+v: %v", expiration, err))
				numFailures += 1
				continue
			}

			numBytes, err := exportEntry(transformed, outFile, extra)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not export expiration %+v: %v", expiration, err))
				numFailures += 1
				continue
			}
			totalNumBytes += numBytes
		}
		outFile.Close()
		cmdLogger.Info("Number of bytes written: ", totalNumBytes)

		printTransformStats(len(expirations), numFailures)
		maybeUpload(gcpCredentials, gcsBucket, path)

	},
}

func init() {
	rootCmd.AddCommand(expirationCmd)
	utils.AddCommonFlags(expirationCmd.Flags())
	utils.AddBucketFlags("expiration", expirationCmd.Flags())
	utils.AddGcsFlags(expirationCmd.Flags())
	expirationCmd.MarkFlagRequired("end-ledger")
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
