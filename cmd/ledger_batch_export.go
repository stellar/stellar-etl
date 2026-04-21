package cmd

import (
	"context"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-etl/v2/internal/input"
	"github.com/stellar/stellar-etl/v2/internal/transform"
	"github.com/stellar/stellar-etl/v2/internal/utils"
)

// processLedgerFunc transforms a single ledger, writing JSON rows to outFile
// via ExportEntry and returning any rows that should be collected for Parquet
// output. attempts and failures are summed across the run by the caller.
type processLedgerFunc func(
	lcm xdr.LedgerCloseMeta,
	env utils.EnvironmentDetails,
	outFile *os.File,
	writeParquet bool,
	extra map[string]string,
) (parquetRows []transform.SchemaParquet, attempts int, failures int)

// runLedgerBatchExport drives the shared pipeline used by every history-archive
// export command: parse flags, prepare the ledger backend, stream batches, and
// for each batch open an output file, fan the batch's ledgers through process,
// then close, upload, and optionally write Parquet. Pass nil for parquetSchema
// if the export has no Parquet output.
func runLedgerBatchExport(
	cmd *cobra.Command,
	exportName string,
	parquetSchema interface{},
	process processLedgerFunc,
) {
	cmdLogger.SetLevel(logrus.InfoLevel)
	commonArgs := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
	cmdLogger.StrictExport = commonArgs.StrictExport
	startNum, batchSize, outputFolder, parquetOutputFolder := utils.MustHistoryArchiveFlags(cmd.Flags(), cmdLogger)
	cloudStorageBucket, cloudCredentials, cloudProvider := utils.MustCloudStorageFlags(cmd.Flags(), cmdLogger)
	env := utils.GetEnvironmentDetails(commonArgs)

	writeParquet := commonArgs.WriteParquet && parquetSchema != nil

	if err := os.MkdirAll(outputFolder, os.ModePerm); err != nil {
		cmdLogger.Fatalf("unable to mkdir %s: %v", outputFolder, err)
	}
	if writeParquet {
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

			path := filepath.Join(outputFolder, exportFilename(batch.BatchStart, batch.BatchEnd+1, exportName))
			outFile := MustOutFile(path)
			var parquetRows []transform.SchemaParquet

			for _, lcm := range batch.Ledgers {
				rows, attempts, failures := process(lcm, env, outFile, writeParquet, commonArgs.Extra)
				totalAttempts += attempts
				totalFailures += failures
				if writeParquet {
					parquetRows = append(parquetRows, rows...)
				}
			}

			outFile.Close()
			MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path)
			if writeParquet {
				parquetPath := filepath.Join(parquetOutputFolder, exportParquetFilename(batch.BatchStart, batch.BatchEnd+1, exportName))
				WriteParquet(parquetRows, parquetPath, parquetSchema)
				MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, parquetPath)
			}
		}
	}
}
