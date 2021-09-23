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

var operationsCmd = &cobra.Command{
	Use:   "export_operations",
	Short: "Exports the operations data over a specified range",
	Long:  `Exports the operations data over a specified range. Each operation is an individual command that mutates the Stellar ledger.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		endNum, useStdout, strictExport, isTest := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		startNum, path, limit := utils.MustArchiveFlags(cmd.Flags(), cmdLogger)

		var outFile *os.File
		if !useStdout {
			outFile = mustOutFile(path)
			cmdLogger.Info("Exporting operations to ", path)
		}

		operations, err := input.GetOperations(startNum, endNum, limit, isTest)
		if err != nil {
			cmdLogger.Fatal("could not read operations: ", err)
		}

		failures := 0
		numBytes := 0
		for _, transformInput := range operations {
			transformed, err := transform.TransformOperation(transformInput.Operation, transformInput.OperationIndex, transformInput.Transaction, transformInput.LedgerSeqNum)
			if err != nil {
				txIndex := transformInput.Transaction.Index
				errMsg := fmt.Sprintf("could not transform operation %d in transaction %d in ledger %d: ", transformInput.OperationIndex, txIndex, transformInput.LedgerSeqNum)
				if strictExport {
					cmdLogger.Fatal(errMsg, err)
				} else {
					cmdLogger.Warning(errMsg, err)
					failures++
					continue
				}

			}

			marshalled, err := json.Marshal(transformed)
			if err != nil {
				txIndex := transformInput.Transaction.Index
				errMsg := fmt.Sprintf("could not json encode operation %d in ledger %d: ", transformInput.OperationIndex, txIndex)
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
					cmdLogger.Info("Error writing operations to file: ", err)
				}
				outFile.WriteString("\n")
				numBytes += nb
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
			printTransformStats(len(operations), failures, printLog)
		}
	},
}

func init() {
	rootCmd.AddCommand(operationsCmd)
	utils.AddCommonFlags(operationsCmd.Flags())
	utils.AddArchiveFlags("operations", operationsCmd.Flags())
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
