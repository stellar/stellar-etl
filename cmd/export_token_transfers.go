package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-etl/v2/internal/transform"
	"github.com/stellar/stellar-etl/v2/internal/utils"
)

var tokenTransfersCmd = &cobra.Command{
	Use:   "export_token_transfer",
	Short: "Exports the token transfer event data over a specified range.",
	Long: `Exports the token transfer event data over a specified range.
Ledgers are processed in batches of batch-size; each batch produces one file
named {start}-{end}-token_transfer.txt in the output folder.`,
	Run: func(cmd *cobra.Command, args []string) {
		runLedgerBatchExport(cmd, "token_transfer", nil,
			func(lcm xdr.LedgerCloseMeta, env utils.EnvironmentDetails, outFile *os.File, _ bool, extra map[string]string) ([]transform.SchemaParquet, int, int) {
				transfers, err := transform.TransformTokenTransfer(lcm, env.NetworkPassphrase)
				if err != nil {
					cmdLogger.LogError(fmt.Errorf("could not transform token transfers for ledger %d: %v", lcm.LedgerSequence(), err))
					return nil, 1, 1
				}
				failures := 0
				for _, transfer := range transfers {
					if _, err := ExportEntry(transfer, outFile, extra); err != nil {
						cmdLogger.LogError(fmt.Errorf("could not export token transfer from ledger %d: %v", lcm.LedgerSequence(), err))
						failures++
						continue
					}
				}
				return nil, 1, failures
			})
	},
}

func init() {
	rootCmd.AddCommand(tokenTransfersCmd)
	utils.AddCommonFlags(tokenTransfersCmd.Flags())
	utils.AddLedgerBatchFlags("token_transfer", tokenTransfersCmd.Flags(), "exported_token_transfer/")
	utils.AddCloudStorageFlags(tokenTransfersCmd.Flags())
	tokenTransfersCmd.MarkFlagRequired("end-ledger")
}
