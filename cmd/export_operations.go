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

var operationsCmd = &cobra.Command{
	Use:   "export_operations",
	Short: "Exports the operations data over a specified range.",
	Long: `Exports the operations data over a specified range. Ledgers are
processed in batches of batch-size; each batch produces one file named
{start}-{end}-operations.txt in the output folder.`,
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

				path := filepath.Join(outputFolder, exportFilename(batch.BatchStart, batch.BatchEnd+1, "operations"))
				parquetPath := filepath.Join(parquetOutputFolder, exportParquetFilename(batch.BatchStart, batch.BatchEnd+1, "operations"))
				outFile := MustOutFile(path)
				var transformedOperations []transform.SchemaParquet

				for _, lcm := range batch.Ledgers {
					opInputs, err := input.OperationsFromLedger(lcm, env.NetworkPassphrase)
					if err != nil {
						cmdLogger.LogError(fmt.Errorf("could not read operations from ledger %d: %v", lcm.LedgerSequence(), err))
						continue
					}
					for _, opInput := range opInputs {
						totalAttempts++
						transformed, err := transform.TransformOperation(opInput.Operation, opInput.OperationIndex, opInput.Transaction, opInput.LedgerSeqNum, opInput.LedgerCloseMeta, env.NetworkPassphrase)
						if err != nil {
							cmdLogger.LogError(fmt.Errorf("could not transform operation %d in transaction %d of ledger %d: %v", opInput.OperationIndex, opInput.Transaction.Index, opInput.LedgerSeqNum, err))
							totalFailures++
							continue
						}
						if _, err := ExportEntry(transformed, outFile, commonArgs.Extra); err != nil {
							cmdLogger.LogError(fmt.Errorf("could not export operation: %v", err))
							totalFailures++
							continue
						}
						if commonArgs.WriteParquet {
							transformedOperations = append(transformedOperations, transformed)
						}
					}
				}

				outFile.Close()
				MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path)
				if commonArgs.WriteParquet {
					WriteParquet(transformedOperations, parquetPath, new(transform.OperationOutputParquet))
					MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, parquetPath)
				}
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(operationsCmd)
	utils.AddCommonFlags(operationsCmd.Flags())
	utils.AddHistoryArchiveFlags("operations", operationsCmd.Flags(), "exported_operations/")
	utils.AddCloudStorageFlags(operationsCmd.Flags())
	operationsCmd.MarkFlagRequired("end-ledger")
}
