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

var effectsCmd = &cobra.Command{
	Use:   "export_effects",
	Short: "Exports the effects data over a specified range.",
	Long: `Exports the effects data over a specified range. Ledgers are
processed in batches of batch-size; each batch produces one file named
{start}-{end}-effects.txt in the output folder.`,
	Run: func(cmd *cobra.Command, args []string) {
		runLedgerBatchExport(cmd, "effects", new(transform.EffectOutputParquet),
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
					ledgerSeq := uint32(txInput.LedgerHistory.Header.LedgerSeq)
					effects, err := transform.TransformEffect(txInput.Transaction, ledgerSeq, txInput.LedgerCloseMeta, env.NetworkPassphrase)
					if err != nil {
						cmdLogger.LogError(fmt.Errorf("could not transform effects for transaction %d in ledger %d: %v", txInput.Transaction.Index, ledgerSeq, err))
						failures++
						continue
					}
					for _, effect := range effects {
						if _, err := ExportEntry(effect, outFile, extra); err != nil {
							cmdLogger.LogError(fmt.Errorf("could not export effect: %v", err))
							failures++
							continue
						}
						if writeParquet {
							rows = append(rows, effect)
						}
					}
				}
				return rows, attempts, failures
			})
	},
}

func init() {
	rootCmd.AddCommand(effectsCmd)
	utils.AddCommonFlags(effectsCmd.Flags())
	utils.AddHistoryArchiveFlags("effects", effectsCmd.Flags(), "exported_effects/")
	utils.AddCloudStorageFlags(effectsCmd.Flags())
	effectsCmd.MarkFlagRequired("end-ledger")
}
