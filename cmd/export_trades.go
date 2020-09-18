package cmd

import (
	"encoding/json"
	"fmt"
	"os"

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
		endNum, useStdout, strictExport := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		startNum, path, limit := utils.MustArchiveFlags(cmd.Flags(), cmdLogger)

		var outFile *os.File
		if !useStdout {
			outFile = mustOutFile(path)
		}

		trades, err := input.GetTrades(startNum, endNum, limit)
		if err != nil {
			cmdLogger.Fatal("could not read trades: ", err)
		}

		failures := 0
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
					outFile.Write(marshalled)
					outFile.WriteString("\n")
				} else {
					fmt.Println(string(marshalled))
				}
			}
		}

		if !strictExport {
			printTransformStats(len(trades), failures)
		}
	},
}

func init() {
	rootCmd.AddCommand(tradesCmd)
	utils.AddCommonFlags(tradesCmd.Flags())
	utils.AddBucketFlags("trades", tradesCmd.Flags())
	tradesCmd.MarkFlagRequired("end-ledger")

	/*
		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			start and end time as a replacement for start and end sequence numbers
	*/
}
