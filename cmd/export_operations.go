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

var operationsCmd = &cobra.Command{
	Use:   "export_operations",
	Short: "Exports the operations data over a specified range.",
	Long: `Exports the operations data over a specified range. Ledgers are
processed in batches of batch-size; each batch produces one file named
{start}-{end}-operations.txt in the output folder.`,
	Run: func(cmd *cobra.Command, args []string) {
		runLedgerBatchExport(cmd, "operations", new(transform.OperationOutputParquet), processOperations)
	},
}

func processOperations(lcm xdr.LedgerCloseMeta, env utils.EnvironmentDetails, outFile *os.File, writeParquet bool, extra map[string]string) ([]transform.SchemaParquet, int, int) {
	opInputs, err := input.OperationsFromLedger(lcm, env.NetworkPassphrase)
	if err != nil {
		cmdLogger.LogError(fmt.Errorf("could not read operations from ledger %d: %v", lcm.LedgerSequence(), err))
		return nil, 0, 0
	}
	var rows []transform.SchemaParquet
	attempts, failures := 0, 0
	for _, opInput := range opInputs {
		attempts++
		transformed, err := transform.TransformOperation(opInput.Operation, opInput.OperationIndex, opInput.Transaction, opInput.LedgerSeqNum, opInput.LedgerCloseMeta, env.NetworkPassphrase)
		if err != nil {
			cmdLogger.LogError(fmt.Errorf("could not transform operation %d in transaction %d of ledger %d: %v", opInput.OperationIndex, opInput.Transaction.Index, opInput.LedgerSeqNum, err))
			failures++
			continue
		}
		if _, err := ExportEntry(transformed, outFile, extra); err != nil {
			cmdLogger.LogError(fmt.Errorf("could not export operation: %v", err))
			failures++
			continue
		}
		if writeParquet {
			rows = append(rows, transformed)
		}
	}
	return rows, attempts, failures
}

func init() {
	rootCmd.AddCommand(operationsCmd)
	utils.AddCommonFlags(operationsCmd.Flags())
	utils.AddLedgerBatchFlags("operations", operationsCmd.Flags(), "exported_operations/")
	utils.AddCloudStorageFlags(operationsCmd.Flags())
	operationsCmd.MarkFlagRequired("end-ledger")
}
