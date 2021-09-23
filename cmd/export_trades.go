package cmd

import (
	"encoding/json"
	"fmt"
	"os"

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
		endNum, useStdout, strictExport, isTest := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		startNum, path, limit := utils.MustArchiveFlags(cmd.Flags(), cmdLogger)

		var outFile *os.File
		if !useStdout {
			outFile = mustOutFile(path)
		}

		trades, err := input.GetTrades(startNum, endNum, limit, isTest)
		if err != nil {
			cmdLogger.Fatal("could not read trades: ", err)
		}

		failures := 0
		numBytes := 0
		for _, tradeInput := range trades {
			trades, err := transform.TransformTrade(tradeInput.OperationIndex, tradeInput.OperationHistoryID, tradeInput.Transaction, tradeInput.CloseTime)
			if err != nil {
				parsedID := toid.Parse(tradeInput.OperationHistoryID)
				locationString := fmt.Sprintf("from ledger %d, transaction %d, operation %d", parsedID.LedgerSequence, parsedID.TransactionOrder, parsedID.OperationOrder)
				if strictExport {
					cmdLogger.Fatalf("could not transform trade (%s): %v", locationString, err)
				} else {
					cmdLogger.Warningf("could not transform trade (%s): %v", locationString, err)
					failures++
					continue
				}
			}

			// We can get multiple trades from each transform, so we need to ensure they are all exported
			for _, transformed := range trades {
				marshalled, err := json.Marshal(transformed)
				if err != nil {
					parsedID := toid.Parse(tradeInput.OperationHistoryID)
					locationString := fmt.Sprintf("from ledger %d, transaction %d, operation %d", parsedID.LedgerSequence, parsedID.TransactionOrder, parsedID.OperationOrder)
					if strictExport {
						cmdLogger.Fatalf("could not JSON encode trade (%s): %v", locationString, err)
					} else {
						cmdLogger.Warningf("could not JSON encode trade (%s): %v", locationString, err)
						failures++
						continue
					}
				}

				if !useStdout {
					nb, err := outFile.Write(marshalled)
					if err != nil {
						cmdLogger.Info("Error writing trades to file: ", err)
					}
					numBytes += nb
					outFile.WriteString("\n")
				} else {
					fmt.Println(string(marshalled))
				}
			}
		}

		if !strictExport {
			printLog := true
			if !useStdout {
				outFile.Close()
				printLog = false
				cmdLogger.Info("Number of bytes written: ", numBytes)
			}
			printTransformStats(len(trades), failures, printLog)
		}
	},
}

func init() {
	rootCmd.AddCommand(tradesCmd)
	utils.AddCommonFlags(tradesCmd.Flags())
	utils.AddArchiveFlags("trades", tradesCmd.Flags())
	tradesCmd.MarkFlagRequired("end-ledger")

	/*
		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			start and end time as a replacement for start and end sequence numbers
	*/
}
