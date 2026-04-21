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

var contractEventsCmd = &cobra.Command{
	Use:   "export_contract_events",
	Short: "Exports the contract events over a specified range.",
	Long: `Exports the contract events over a specified range. Ledgers are
processed in batches of batch-size; each batch produces one file named
{start}-{end}-contract_events.txt in the output folder.`,
	Run: func(cmd *cobra.Command, args []string) {
		runLedgerBatchExport(cmd, "contract_events", new(transform.ContractEventOutputParquet),
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
					events, err := transform.TransformContractEvent(txInput.Transaction, txInput.LedgerHistory)
					if err != nil {
						ledgerSeq := txInput.LedgerHistory.Header.LedgerSeq
						cmdLogger.LogError(fmt.Errorf("could not transform contract events for transaction %d in ledger %d: %v", txInput.Transaction.Index, ledgerSeq, err))
						failures++
						continue
					}
					for _, event := range events {
						if _, err := ExportEntry(event, outFile, extra); err != nil {
							cmdLogger.LogError(fmt.Errorf("could not export contract event: %v", err))
							failures++
							continue
						}
						if writeParquet {
							rows = append(rows, event)
						}
					}
				}
				return rows, attempts, failures
			})
	},
}

func init() {
	rootCmd.AddCommand(contractEventsCmd)
	utils.AddCommonFlags(contractEventsCmd.Flags())
	utils.AddHistoryArchiveFlags("contract_events", contractEventsCmd.Flags(), "exported_contract_events/")
	utils.AddCloudStorageFlags(contractEventsCmd.Flags())
	contractEventsCmd.MarkFlagRequired("start-ledger")
	contractEventsCmd.MarkFlagRequired("end-ledger")
}
