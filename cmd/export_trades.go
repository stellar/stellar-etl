package cmd

import (
	"encoding/json"
	"fmt"
	"os"

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
		startNum, endNum, limit, path, useStdout := utils.MustBasicFlags(cmd.Flags(), cmdLogger)

		var outFile *os.File
		if !useStdout {
			outFile = mustOutFile(path)
		}

		trades, err := input.GetTrades(startNum, endNum, limit)
		if err != nil {
			cmdLogger.Fatal("could not read trades: ", err)
		}

		for _, tradeInput := range trades {
			trades, err := transform.TransformTrade(tradeInput.OperationIndex, tradeInput.OperationHistoryID, tradeInput.Transaction, tradeInput.CloseTime)
			if err != nil {
				cmdLogger.Fatal("could not transform trade ", err)
			}

			// Each operation, when transformed, results in multiple trades, each of which needs to be exported
			for _, transformed := range trades {
				marshalled, err := json.Marshal(transformed)
				if err != nil {
					cmdLogger.Fatal("could not json encode trade ", err)
				}

				if !useStdout {
					outFile.Write(marshalled)
					outFile.WriteString("\n")
				} else {
					fmt.Println(string(marshalled))
				}
			}

		}
	},
}

func init() {
	rootCmd.AddCommand(tradesCmd)
	utils.AddBasicFlags("trades", tradesCmd.Flags())
	tradesCmd.MarkFlagRequired("end-ledger")

	/*
		Current flags:
			start-ledger: the ledger sequence number for the beginning of the export period
			end-ledger: the ledger sequence number for the end of the export range (*required)

			limit: maximum number of trades to export

			output-file: filename of the output file
			stdout: if set, output is printed to stdout

		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			start and end time as a replacement for start and end sequence numbers
	*/
}