package cmd

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"
)

var assetsCmd = &cobra.Command{
	Use:   "export_assets",
	Short: "Exports the assets data over a specified range",
	Long:  `Exports the assets that are created from payment operations over a specified ledger range`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		endNum, strictExport, isTest, isFuture, extra := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = strictExport
		startNum, path, limit := utils.MustArchiveFlags(cmd.Flags(), cmdLogger)
		cloudStorageBucket, cloudCredentials, cloudProvider := utils.MustCloudStorageFlags(cmd.Flags(), cmdLogger)

		outFile := mustOutFile(path)

		paymentOps, err := input.GetPaymentOperations(startNum, endNum, limit, isTest, isFuture)
		if err != nil {
			cmdLogger.Fatal("could not read asset: ", err)
		}

		// With seenIDs, the code doesn't export duplicate assets within a single export. Note that across exports, assets may be duplicated
		seenIDs := map[uint64]bool{}
		numFailures := 0
		totalNumBytes := 0
		for _, transformInput := range paymentOps {
			transformed, err := transform.TransformAsset(transformInput.Operation, transformInput.OperationIndex, transformInput.TransactionIndex, transformInput.LedgerSeqNum)
			if err != nil {
				txIndex := transformInput.TransactionIndex
				cmdLogger.LogError(fmt.Errorf("could not extract asset from operation %d in transaction %d in ledger %d: ", transformInput.OperationIndex, txIndex, transformInput.LedgerSeqNum))
				numFailures += 1
				continue
			}

			// if we have seen the asset already, do not export it
			if _, exists := seenIDs[transformed.AssetID]; exists {
				continue
			}

			seenIDs[transformed.AssetID] = true
			numBytes, err := exportEntry(transformed, outFile, extra)
			if err != nil {
				cmdLogger.Error(err)
				numFailures += 1
				continue
			}
			totalNumBytes += numBytes
		}

		outFile.Close()
		cmdLogger.Infof("%d bytes written to %s", totalNumBytes, outFile.Name())

		printTransformStats(len(paymentOps), numFailures)

		maybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path)
	},
}

func init() {
	rootCmd.AddCommand(assetsCmd)
	utils.AddCommonFlags(assetsCmd.Flags())
	utils.AddArchiveFlags("assets", assetsCmd.Flags())
	utils.AddCloudStorageFlags(assetsCmd.Flags())
	assetsCmd.MarkFlagRequired("end-ledger")

	/*
		Current flags:
			start-ledger: the ledger sequence number for the beginning of the export period
			end-ledger: the ledger sequence number for the end of the export range (required)

			limit: maximum number of operations to export; default to 6,000,000
				each transaction can have up to 100 operations
				each ledger can have up to 1000 transactions
				there are 60 new ledgers in a 5 minute period

			output-file: filename of the output file

		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			start and end time as a replacement for start and end sequence numbers
	*/
}
