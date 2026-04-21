package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-etl/v2/internal/input"
	"github.com/stellar/stellar-etl/v2/internal/transform"
	"github.com/stellar/stellar-etl/v2/internal/utils"
)

var transactionsCmd = &cobra.Command{
	Use:   "export_transactions",
	Short: "Exports the transaction data over a specified range.",
	Long: `Exports the transaction data over a specified range. Ledgers are
processed in batches of batch-size. Each batch produces one file named
{start}-{end}-transactions.txt (and .parquet when --write-parquet is set) in
the output folder, which is uploaded before the next batch is processed.`,
	Run: func(cmd *cobra.Command, args []string) {
		runLedgerBatchExport(cmd, "transactions", new(transform.TransactionOutputParquet),
			func(lcm xdr.LedgerCloseMeta, env utils.EnvironmentDetails, outFile *os.File, writeParquet bool, extra map[string]string) ([]transform.SchemaParquet, int, int) {
				txInputs, err := input.TransactionsFromLedger(lcm, env.NetworkPassphrase)
				if err != nil {
					cmdLogger.LogError(fmt.Errorf("could not read transactions from ledger %d: %v", lcm.LedgerSequence(), err))
					return nil, 0, 0
				}
				var rows []transform.SchemaParquet
				attempts, failures := 0, 0
				for _, txInput := range txInputs {
					attempts++
					transformed, err := transform.TransformTransaction(txInput.Transaction, txInput.LedgerHistory)
					if err != nil {
						ledgerSeq := txInput.LedgerHistory.Header.LedgerSeq
						cmdLogger.LogError(fmt.Errorf("could not transform transaction %d in ledger %d: %v", txInput.Transaction.Index, ledgerSeq, err))
						failures++
						continue
					}
					if _, err := ExportEntry(transformed, outFile, extra); err != nil {
						cmdLogger.LogError(fmt.Errorf("could not export transaction: %v", err))
						failures++
						continue
					}
					if writeParquet {
						rows = append(rows, transformed)
					}
				}
				return rows, attempts, failures
			})
	},
}

func init() {
	rootCmd.AddCommand(transactionsCmd)
	utils.AddCommonFlags(transactionsCmd.Flags())
	utils.AddHistoryArchiveFlags("transactions", transactionsCmd.Flags(), "exported_transactions/")
	utils.AddCloudStorageFlags(transactionsCmd.Flags())
	transactionsCmd.MarkFlagRequired("end-ledger")
}
