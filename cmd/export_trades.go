package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-etl/v2/internal/input"
	"github.com/stellar/stellar-etl/v2/internal/toid"
	"github.com/stellar/stellar-etl/v2/internal/transform"
	"github.com/stellar/stellar-etl/v2/internal/utils"
)

// tradesCmd represents the trades command
var tradesCmd = &cobra.Command{
	Use:   "export_trades",
	Short: "Exports the trade data over a specified range.",
	Long: `Exports trade data within the specified range. Ledgers are
processed in batches of batch-size; each batch produces one file named
{start}-{end}-trades.txt in the output folder.`,
	Run: func(cmd *cobra.Command, args []string) {
		runLedgerBatchExport(cmd, "trades", new(transform.TradeOutputParquet),
			func(lcm xdr.LedgerCloseMeta, env utils.EnvironmentDetails, outFile *os.File, writeParquet bool, extra map[string]string) ([]transform.SchemaParquet, int, int) {
				tradeInputs, err := input.TradesFromLedger(lcm, env.NetworkPassphrase)
				if err != nil {
					cmdLogger.LogError(fmt.Errorf("could not read trades from ledger %d: %v", lcm.LedgerSequence(), err))
					return nil, 0, 0
				}
				var rows []transform.SchemaParquet
				attempts, failures := 0, 0
				for _, tradeInput := range tradeInputs {
					attempts++
					trades, err := transform.TransformTrade(tradeInput.OperationIndex, tradeInput.OperationHistoryID, tradeInput.Transaction, tradeInput.CloseTime)
					if err != nil {
						parsedID := toid.Parse(tradeInput.OperationHistoryID)
						cmdLogger.LogError(fmt.Errorf("from ledger %d, transaction %d, operation %d: %v", parsedID.LedgerSequence, parsedID.TransactionOrder, parsedID.OperationOrder, err))
						failures++
						continue
					}
					for _, trade := range trades {
						if _, err := ExportEntry(trade, outFile, extra); err != nil {
							cmdLogger.LogError(fmt.Errorf("could not export trade: %v", err))
							failures++
							continue
						}
						if writeParquet {
							rows = append(rows, trade)
						}
					}
				}
				return rows, attempts, failures
			})
	},
}

func init() {
	rootCmd.AddCommand(tradesCmd)
	utils.AddCommonFlags(tradesCmd.Flags())
	utils.AddLedgerBatchFlags("trades", tradesCmd.Flags(), "exported_trades/")
	utils.AddCloudStorageFlags(tradesCmd.Flags())
	tradesCmd.MarkFlagRequired("end-ledger")
}
