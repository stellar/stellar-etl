package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"
)

func createOutputFile(filepath string) error {
	var _, err = os.Stat(filepath)
	if os.IsNotExist(err) {
		var _, err = os.Create(filepath)
		if err != nil {
			return err
		}
	}

	return nil
}

func mustOutFile(path string) *os.File {
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		cmdLogger.Fatal("could not get absolute filepath: ", err)
	}

	err = os.MkdirAll(filepath.Dir(path), os.ModePerm)
	if err != nil {
		cmdLogger.Fatalf("could not create directory %s: ", path, err)
	}

	err = createOutputFile(absolutePath)
	if err != nil {
		cmdLogger.Fatal("could not create output file: ", err)
	}

	outFile, err := os.OpenFile(absolutePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		cmdLogger.Fatal("error in opening output file: ", err)
	}

	return outFile
}

func exportEntry(entry interface{}, outFile *os.File, extra map[string]string) (int, error) {
	// This entra marshalling/unmarshalling is silly, but it's required to properly handle the null.[String|Int*] types, and add the extra fields.
	m, err := json.Marshal(entry)
	if err != nil {
		cmdLogger.Errorf("Error marshalling %+v: %v ", entry, err)
	}
	i := map[string]interface{}{}
	err = json.Unmarshal(m, &i)
	if err != nil {
		cmdLogger.Errorf("Error unmarshalling %+v: %v ", i, err)
	}
	for k, v := range extra {
		i[k] = v
	}

	marshalled, err := json.Marshal(i)
	if err != nil {
		return 0, fmt.Errorf("could not json encode %+v: %s", entry, err)
	}
	cmdLogger.Debugf("Writing entry to %s", outFile.Name)
	numBytes, err := outFile.Write(marshalled)
	if err != nil {
		cmdLogger.Errorf("Error writing %+v to file: ", entry, err)
	}
	newLineNumBytes, err := outFile.WriteString("\n")
	if err != nil {
		cmdLogger.Error("Error writing new line to file %s: ", outFile.Name, err)
	}
	return numBytes + newLineNumBytes, nil
}

// Prints the number of attempted, failed, and successful transformations as a JSON object
func printTransformStats(attempts, failures int) {
	resultsMap := map[string]int{
		"attempted_transforms":  attempts,
		"failed_transforms":     failures,
		"successful_transforms": attempts - failures,
	}

	results, err := json.Marshal(resultsMap)
	if err != nil {
		cmdLogger.Fatal("Could not marshal results: ", err)
	}

	cmdLogger.Info(string(results))
}

func exportFilename(start, end uint32, dataType string) string {
	return fmt.Sprintf("%d-%d-%s.txt", start, end-1, dataType)
}

func uploadToGcs(credentialsPath, bucket, path string) error {
	// Use credentials file in dev/local runs. Otherwise, derive credentials from the service account.
	if len(credentialsPath) > 0 {
		os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credentialsPath)
		cmdLogger.Infof("Using credentials found at: %s", credentialsPath)
	}

	reader, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %v", path, err)
	}

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Hour)
	defer cancel()

	wc := client.Bucket(bucket).Object(path).NewWriter(ctx)

	uploadLocation := fmt.Sprintf("gs://%s/%s", bucket, path)
	cmdLogger.Infof("Uploading %s to %s", path, uploadLocation)

	var written int64
	if written, err = io.Copy(wc, reader); err != nil {
		return fmt.Errorf("unable to copy: %v", err)
	}
	err = wc.Close()
	if err != nil {
		return err
	}

	cmdLogger.Infof("Successfully uploaded %d bytes to gs://%s/%s", written, bucket, path)
	return nil
}

func maybeUpload(gcpCredentials, gcsBucket, path string) {
	if len(gcsBucket) > 0 {
		err := uploadToGcs(gcpCredentials, gcsBucket, path)
		if err != nil {
			cmdLogger.Errorf("Unable to upload output to GCS: %s", err)
			return
		}
		err = os.RemoveAll(path)
		if err != nil {
			cmdLogger.Errorf("Unable to remove %s: %s", path, err)
			return
		}
		cmdLogger.Infof("Successfully deleted %s", path)
	}
}

var ledgersCmd = &cobra.Command{
	Use:   "export_ledgers",
	Short: "Exports the ledger data.",
	Long:  `Exports ledger data within the specified range to an output file. Encodes ledgers as JSON objects and exports them to the output file.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		endNum, strictExport, isTest, extra := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = strictExport
		startNum, path, limit := utils.MustArchiveFlags(cmd.Flags(), cmdLogger)
		gcsBucket, gcpCredentials := utils.MustGcsFlags(cmd.Flags(), cmdLogger)

		ledgers, err := input.GetLedgers(startNum, endNum, limit, isTest)
		if err != nil {
			cmdLogger.Fatal("could not read ledgers: ", err)
		}

		outFile := mustOutFile(path)

		numFailures := 0
		totalNumBytes := 0
		for i, lcm := range ledgers {
			transformed, err := transform.TransformLedger(lcm)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not json transform ledger %d: %s", startNum+uint32(i), err))
				numFailures += 1
				continue
			}

			numBytes, err := exportEntry(transformed, outFile, extra)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not export ledger %d: %s", startNum+uint32(i), err))
				numFailures += 1
				continue
			}
			totalNumBytes += numBytes
		}

		outFile.Close()
		cmdLogger.Info("Number of bytes written: ", totalNumBytes)

		printTransformStats(len(ledgers), numFailures)

		maybeUpload(gcpCredentials, gcsBucket, path)
	},
}

func init() {
	rootCmd.AddCommand(ledgersCmd)
	utils.AddCommonFlags(ledgersCmd.Flags())
	utils.AddArchiveFlags("ledgers", ledgersCmd.Flags())
	utils.AddGcsFlags(ledgersCmd.Flags())
	ledgersCmd.MarkFlagRequired("end-ledger")
	/*
		Current flags:
			start-ledger: the ledger sequence number for the beginning of the export period
			end-ledger: the ledger sequence number for the end of the export range (required)

			limit: maximum number of ledgers to export; default to 60 (1 ledger per 5 seconds over our 5 minute update period)
			output-file: filename of the output file

		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			start and end time as a replacement for start and end sequence numbers
	*/
}
