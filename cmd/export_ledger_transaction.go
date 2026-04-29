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

var ledgerTransactionCmd = &cobra.Command{
	Use:   "export_ledger_transaction",
	Short: "Exports the ledger_transaction data over a specified range.",
	Long: `Exports the ledger_transaction data over a specified range. Ledgers
are processed in batches of batch-size; each batch produces one file named
{start}-{end}-ledger_transaction.txt in the output folder.`,
	Run: func(cmd *cobra.Command, args []string) {
		runLedgerBatchExport(cmd, "ledger_transaction", nil, processLedgerTransaction)
	},
}

func processLedgerTransaction(lcm xdr.LedgerCloseMeta, env utils.EnvironmentDetails, outFile *os.File, _ bool, extra map[string]string) ([]transform.SchemaParquet, int, int) {
	txInputs, err := input.TransactionsFromLedger(lcm, env.NetworkPassphrase)
	if err != nil {
		cmdLogger.LogError(fmt.Errorf("could not read transactions from ledger %d: %v", lcm.LedgerSequence(), err))
		return nil, 0, 0
	}
	attempts, failures := 0, 0
	for _, txInput := range txInputs {
		attempts++
		transformed, err := transform.TransformLedgerTransaction(txInput.Transaction, txInput.LedgerHistory)
		if err != nil {
			ledgerSeq := txInput.LedgerHistory.Header.LedgerSeq
			cmdLogger.LogError(fmt.Errorf("could not transform ledger_transaction %d in ledger %d: %v", txInput.Transaction.Index, ledgerSeq, err))
			failures++
			continue
		}
		if _, err := ExportEntry(transformed, outFile, extra); err != nil {
			cmdLogger.LogError(fmt.Errorf("could not export ledger_transaction: %v", err))
			failures++
			continue
		}
	}
	return nil, attempts, failures
}

func init() {
	rootCmd.AddCommand(ledgerTransactionCmd)
	utils.AddCommonFlags(ledgerTransactionCmd.Flags())
	utils.AddLedgerBatchFlags("ledger_transaction", ledgerTransactionCmd.Flags(), "exported_ledger_transaction/")
	utils.AddCloudStorageFlags(ledgerTransactionCmd.Flags())
	ledgerTransactionCmd.MarkFlagRequired("end-ledger")
}
