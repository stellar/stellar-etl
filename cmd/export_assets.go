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

var assetsCmd = &cobra.Command{
	Use:   "export_assets",
	Short: "Exports the assets data over a specified range.",
	Long: `Exports the assets that are created from payment operations over a
specified ledger range. Ledgers are processed in batches of batch-size; each
batch produces one file named {start}-{end}-assets.txt in the output folder.
Duplicate assets are deduplicated across the entire run.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		commonArgs := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = commonArgs.StrictExport
		startNum, batchSize, outputFolder, parquetOutputFolder := utils.MustHistoryArchiveFlags(cmd.Flags(), cmdLogger)
		cloudStorageBucket, cloudCredentials, cloudProvider := utils.MustCloudStorageFlags(cmd.Flags(), cmdLogger)
		env := utils.GetEnvironmentDetails(commonArgs)

		if err := os.MkdirAll(outputFolder, os.ModePerm); err != nil {
			cmdLogger.Fatalf("unable to mkdir %s: %v", outputFolder, err)
		}
		if commonArgs.WriteParquet {
			if err := os.MkdirAll(parquetOutputFolder, os.ModePerm); err != nil {
				cmdLogger.Fatalf("unable to mkdir %s: %v", parquetOutputFolder, err)
			}
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

		// seenIDs persists across batches so the same asset never appears in
		// two different output files within a single run.
		seenIDs := map[int64]bool{}
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

				path := filepath.Join(outputFolder, exportFilename(batch.BatchStart, batch.BatchEnd+1, "assets"))
				parquetPath := filepath.Join(parquetOutputFolder, exportParquetFilename(batch.BatchStart, batch.BatchEnd+1, "assets"))
				outFile := MustOutFile(path)
				var transformedAssets []transform.SchemaParquet

				for _, lcm := range batch.Ledgers {
					paymentOps := input.PaymentOperationsFromLedger(lcm)
					for _, assetInput := range paymentOps {
						totalAttempts++
						transformed, err := transform.TransformAsset(assetInput.Operation, assetInput.OperationIndex, assetInput.TransactionIndex, assetInput.LedgerSeqNum, assetInput.LedgerCloseMeta)
						if err != nil {
							cmdLogger.LogError(fmt.Errorf("could not transform asset from operation %d transaction %d ledger %d: %v", assetInput.OperationIndex, assetInput.TransactionIndex, assetInput.LedgerSeqNum, err))
							totalFailures++
							continue
						}

						if _, exists := seenIDs[transformed.AssetID]; exists {
							continue
						}
						seenIDs[transformed.AssetID] = true

						if _, err := ExportEntry(transformed, outFile, commonArgs.Extra); err != nil {
							cmdLogger.LogError(fmt.Errorf("could not export asset: %v", err))
							totalFailures++
							continue
						}
						if commonArgs.WriteParquet {
							transformedAssets = append(transformedAssets, transformed)
						}
					}
				}

				outFile.Close()
				MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path)
				if commonArgs.WriteParquet {
					WriteParquet(transformedAssets, parquetPath, new(transform.AssetOutputParquet))
					MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, parquetPath)
				}
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(assetsCmd)
	utils.AddCommonFlags(assetsCmd.Flags())
	utils.AddHistoryArchiveFlags("assets", assetsCmd.Flags(), "exported_assets/")
	utils.AddCloudStorageFlags(assetsCmd.Flags())
	assetsCmd.MarkFlagRequired("end-ledger")
}
