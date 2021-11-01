package cmd

import (
	"encoding/json"
	"fmt"
	"os"

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
		endNum, useStdout, strictExport, isTest := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		startNum, path, limit := utils.MustArchiveFlags(cmd.Flags(), cmdLogger)

		paymentOps, err := input.GetPaymentOperations(startNum, endNum, limit, isTest)
		if err != nil {
			cmdLogger.Fatal("could not read assets: ", err)
		}

		var outFile *os.File
		if !useStdout {
			outFile = mustOutFile(path)
		}

		// With seenIDs, the code doesn't export duplicate assets within a single export. Note that across exports, assets may be duplicated
		seenIDs := map[uint64]bool{}
		failures := 0
		numBytes := 0
		for _, transformInput := range paymentOps {
			transformed, err := transform.TransformAsset(transformInput.Operation, transformInput.OperationIndex, transformInput.Transaction, transformInput.LedgerSeqNum)
			if err != nil {
				txIndex := transformInput.Transaction.Index
				errMsg := fmt.Sprintf("could not extract asset from operation %d in transaction %d in ledger %d: ", transformInput.OperationIndex, txIndex, transformInput.LedgerSeqNum)
				if strictExport {
					cmdLogger.Fatal(errMsg, err)
				} else {
					cmdLogger.Warning(errMsg, err)
					failures++
					continue
				}

			}

			// if we have seen the asset already, do not export it
			if _, exists := seenIDs[transformed.AssetID]; exists {
				continue
			} else {
				seenIDs[transformed.AssetID] = true
			}

			marshalled, err := json.Marshal(transformed)
			if err != nil {
				txIndex := transformInput.Transaction.Index
				errMsg := fmt.Sprintf("could not json encode asset from operation %d in ledger %d: ", transformInput.OperationIndex, txIndex)
				if strictExport {
					cmdLogger.Fatal(errMsg, err)
				} else {
					cmdLogger.Warning(errMsg, err)
					failures++
					continue
				}
			}

			if !useStdout {
				nb, err := outFile.Write(marshalled)
				if err != nil {
					cmdLogger.Info("Error writing assets to file: ", err)
				}
				numBytes += nb
				outFile.WriteString("\n")
			} else {
				fmt.Println(string(marshalled))
			}
		}

		if !strictExport {
			printLog := true
			if !useStdout {
				outFile.Close()
				printLog = false
				cmdLogger.Info("Number of bytes written: ", numBytes)
			}
			printTransformStats(len(paymentOps), failures, printLog)
		}
	},
}

func init() {
	rootCmd.AddCommand(assetsCmd)
	utils.AddCommonFlags(assetsCmd.Flags())
	utils.AddArchiveFlags("assets", assetsCmd.Flags())
	operationsCmd.MarkFlagRequired("end-ledger")

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
