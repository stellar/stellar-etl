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

var assetsCmd = &cobra.Command{
	Use:   "export_assets",
	Short: "Exports the assets data over a specified range.",
	Long: `Exports the assets that are created from payment operations over a
specified ledger range. Ledgers are processed in batches of batch-size; each
batch produces one file named {start}-{end}-assets.txt in the output folder.
Duplicate assets are deduplicated across the entire run.`,
	Run: func(cmd *cobra.Command, args []string) {
		runLedgerBatchExport(cmd, "assets", new(transform.AssetOutputParquet), newAssetsProcessor())
	},
}

// newAssetsProcessor returns a processor closure that dedupes by AssetID across
// every batch of a single run, so the same asset never appears in two different
// output files.
func newAssetsProcessor() processLedgerFunc {
	seenIDs := map[int64]bool{}
	return func(lcm xdr.LedgerCloseMeta, _ utils.EnvironmentDetails, outFile *os.File, writeParquet bool, extra map[string]string) ([]transform.SchemaParquet, int, int) {
		var rows []transform.SchemaParquet
		attempts, failures := 0, 0
		for _, assetInput := range input.PaymentOperationsFromLedger(lcm) {
			attempts++
			transformed, err := transform.TransformAsset(assetInput.Operation, assetInput.OperationIndex, assetInput.TransactionIndex, assetInput.LedgerSeqNum, assetInput.LedgerCloseMeta)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not transform asset from operation %d transaction %d ledger %d: %v", assetInput.OperationIndex, assetInput.TransactionIndex, assetInput.LedgerSeqNum, err))
				failures++
				continue
			}
			if seenIDs[transformed.AssetID] {
				continue
			}
			seenIDs[transformed.AssetID] = true
			if _, err := ExportEntry(transformed, outFile, extra); err != nil {
				cmdLogger.LogError(fmt.Errorf("could not export asset: %v", err))
				failures++
				continue
			}
			if writeParquet {
				rows = append(rows, transformed)
			}
		}
		return rows, attempts, failures
	}
}

func init() {
	rootCmd.AddCommand(assetsCmd)
	utils.AddCommonFlags(assetsCmd.Flags())
	utils.AddLedgerBatchFlags("assets", assetsCmd.Flags(), "exported_assets/")
	utils.AddCloudStorageFlags(assetsCmd.Flags())
	assetsCmd.MarkFlagRequired("end-ledger")
}
