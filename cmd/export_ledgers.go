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

var ledgersCmd = &cobra.Command{
	Use:   "export_ledgers",
	Short: "Exports the ledger data over a specified range.",
	Long: `Exports ledger data within the specified range. Ledgers are
processed in batches of batch-size; each batch produces one file named
{start}-{end}-ledgers.txt in the output folder.`,
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

				path := filepath.Join(outputFolder, exportFilename(batch.BatchStart, batch.BatchEnd+1, "ledgers"))
				parquetPath := filepath.Join(parquetOutputFolder, exportParquetFilename(batch.BatchStart, batch.BatchEnd+1, "ledgers"))
				outFile := MustOutFile(path)
				var transformedLedgers []transform.SchemaParquet

				for _, lcm := range batch.Ledgers {
					totalAttempts++
					ledger := input.HistoryArchiveLedgerFromLCM(lcm)
					transformed, err := transform.TransformLedger(ledger, lcm)
					if err != nil {
						cmdLogger.LogError(fmt.Errorf("could not transform ledger %d: %v", lcm.LedgerSequence(), err))
						totalFailures++
						continue
					}
					if _, err := ExportEntry(transformed, outFile, commonArgs.Extra); err != nil {
						cmdLogger.LogError(fmt.Errorf("could not export ledger %d: %v", lcm.LedgerSequence(), err))
						totalFailures++
						continue
					}
					if commonArgs.WriteParquet {
						transformedLedgers = append(transformedLedgers, transformed)
					}
				}

				outFile.Close()
				MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path)
				if commonArgs.WriteParquet {
					WriteParquet(transformedLedgers, parquetPath, new(transform.LedgerOutputParquet))
					MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, parquetPath)
				}
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(ledgersCmd)
	utils.AddCommonFlags(ledgersCmd.Flags())
	utils.AddHistoryArchiveFlags("ledgers", ledgersCmd.Flags(), "exported_ledgers/")
	utils.AddCloudStorageFlags(ledgersCmd.Flags())
	ledgersCmd.MarkFlagRequired("end-ledger")
}
