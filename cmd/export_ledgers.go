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

var ledgersCmd = &cobra.Command{
	Use:   "export_ledgers",
	Short: "Exports the ledger data over a specified range.",
	Long: `Exports ledger data within the specified range. Ledgers are
processed in batches of batch-size; each batch produces one file named
{start}-{end}-ledgers.txt in the output folder.`,
	Run: func(cmd *cobra.Command, args []string) {
		runLedgerBatchExport(cmd, "ledgers", new(transform.LedgerOutputParquet),
			func(lcm xdr.LedgerCloseMeta, _ utils.EnvironmentDetails, outFile *os.File, writeParquet bool, extra map[string]string) ([]transform.SchemaParquet, int, int) {
				ledger := input.HistoryArchiveLedgerFromLCM(lcm)
				transformed, err := transform.TransformLedger(ledger, lcm)
				if err != nil {
					cmdLogger.LogError(fmt.Errorf("could not transform ledger %d: %v", lcm.LedgerSequence(), err))
					return nil, 1, 1
				}
				if _, err := ExportEntry(transformed, outFile, extra); err != nil {
					cmdLogger.LogError(fmt.Errorf("could not export ledger %d: %v", lcm.LedgerSequence(), err))
					return nil, 1, 1
				}
				if writeParquet {
					return []transform.SchemaParquet{transformed}, 1, 0
				}
				return nil, 1, 0
			})
	},
}

func init() {
	rootCmd.AddCommand(ledgersCmd)
	utils.AddCommonFlags(ledgersCmd.Flags())
	utils.AddLedgerBatchFlags("ledgers", ledgersCmd.Flags(), "exported_ledgers/")
	utils.AddCloudStorageFlags(ledgersCmd.Flags())
	ledgersCmd.MarkFlagRequired("end-ledger")
}
