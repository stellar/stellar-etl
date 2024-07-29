package cmd

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/stellar/stellar-etl/internal/toid"

	"github.com/spf13/cobra"
	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"
)

// tradesCmd represents the trades command
var tradesCmd = &cobra.Command{
	Use:   "export_trades",
	Short: "Exports the trade data",
	Long:  `Exports trade data within the specified range to an output file`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		commonArgs := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = commonArgs.StrictExport
		startNum, path, parquetPath, limit := utils.MustArchiveFlags(cmd.Flags(), cmdLogger)
		env := utils.GetEnvironmentDetails(commonArgs)
		cloudStorageBucket, cloudCredentials, cloudProvider := utils.MustCloudStorageFlags(cmd.Flags(), cmdLogger)

		trades, err := input.GetTrades(startNum, commonArgs.EndNum, limit, env, commonArgs.UseCaptiveCore)
		if err != nil {
			cmdLogger.Fatal("could not read trades ", err)
		}

		outFile := mustOutFile(path)
		numFailures := 0
		totalNumBytes := 0
		var transformedTrades []transform.SchemaParquet
		for _, tradeInput := range trades {
			trades, err := transform.TransformTrade(tradeInput.OperationIndex, tradeInput.OperationHistoryID, tradeInput.Transaction, tradeInput.CloseTime)
			if err != nil {
				parsedID := toid.Parse(tradeInput.OperationHistoryID)
				cmdLogger.LogError(fmt.Errorf("from ledger %d, transaction %d, operation %d: %v", parsedID.LedgerSequence, parsedID.TransactionOrder, parsedID.OperationOrder, err))
				numFailures += 1
				continue
			}

			for _, transformed := range trades {
				numBytes, err := exportEntry(transformed, outFile, commonArgs.Extra)
				if err != nil {
					cmdLogger.LogError(err)
					numFailures += 1
					continue
				}
				totalNumBytes += numBytes

				if commonArgs.WriteParquet {
					transformedTrades = append(transformedTrades, transformed)
				}
			}
		}

		outFile.Close()
		cmdLogger.Info("Number of bytes written: ", totalNumBytes)

		printTransformStats(len(trades), numFailures)

		maybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path)

		if commonArgs.WriteParquet {
			maybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, parquetPath)
			writeParquet(transformedTrades, parquetPath, new(transform.TradeOutputParquet))
		}
	},
}

func init() {
	rootCmd.AddCommand(tradesCmd)
	utils.AddCommonFlags(tradesCmd.Flags())
	utils.AddArchiveFlags("trades", tradesCmd.Flags())
	utils.AddCloudStorageFlags(tradesCmd.Flags())
	tradesCmd.MarkFlagRequired("end-ledger")

	/*
		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			start and end time as a replacement for start and end sequence numbers
	*/
}
