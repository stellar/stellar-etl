package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/stellar-etl/v2/internal/input"
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
		cmdLogger.SetLevel(logrus.InfoLevel)
		commonArgs := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = commonArgs.StrictExport
		startNum, batchSize, outputFolder, _ := utils.MustHistoryArchiveFlags(cmd.Flags(), cmdLogger)
		cloudStorageBucket, cloudCredentials, cloudProvider := utils.MustCloudStorageFlags(cmd.Flags(), cmdLogger)
		env := utils.GetEnvironmentDetails(commonArgs)

		if err := os.MkdirAll(outputFolder, os.ModePerm); err != nil {
			cmdLogger.Fatalf("unable to mkdir %s: %v", outputFolder, err)
		}
		if batchSize == 0 {
			cmdLogger.Fatalf("batch-size (%d) must be greater than 0", batchSize)
		}

		ctx := context.Background()
		backend, err := utils.CreateLedgerBackend(ctx, commonArgs.UseCaptiveCore, env)
		if err != nil {
			cmdLogger.Fatal("could not create ledger backend: ", err)
		}
		if err := backend.PrepareRange(ctx, ledgerbackend.BoundedRange(startNum, commonArgs.EndNum)); err != nil {
			cmdLogger.Fatal("could not prepare ledger range: ", err)
		}

		batchChan := make(chan input.LedgerBatch)
		closeChan := make(chan int)
		go input.StreamLedgerBatches(&backend, startNum, commonArgs.EndNum, batchSize, batchChan, closeChan, cmdLogger)

		totalAttempts, totalFailures := 0, 0
		for {
			select {
			case <-closeChan:
				PrintTransformStats(totalAttempts, totalFailures)
				return
			case batch, ok := <-batchChan:
				if !ok {
					continue
				}

				path := filepath.Join(outputFolder, exportFilename(batch.BatchStart, batch.BatchEnd+1, "token_transfer"))
				outFile := MustOutFile(path)

				for _, lcm := range batch.Ledgers {
					totalAttempts++
					transfers, err := transform.TransformTokenTransfer(lcm, env.NetworkPassphrase)
					if err != nil {
						cmdLogger.LogError(fmt.Errorf("could not transform token transfers for ledger %d: %v", lcm.LedgerSequence(), err))
						totalFailures++
						continue
					}
					for _, transfer := range transfers {
						if _, err := ExportEntry(transfer, outFile, commonArgs.Extra); err != nil {
							cmdLogger.LogError(fmt.Errorf("could not export token transfer from ledger %d: %v", lcm.LedgerSequence(), err))
							totalFailures++
							continue
						}
					}
				}

				outFile.Close()
				MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path)
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(tokenTransfersCmd)
	utils.AddCommonFlags(tokenTransfersCmd.Flags())
	utils.AddHistoryArchiveFlags("token_transfer", tokenTransfersCmd.Flags(), "exported_token_transfer/")
	utils.AddCloudStorageFlags(tokenTransfersCmd.Flags())
	tokenTransfersCmd.MarkFlagRequired("end-ledger")
}
