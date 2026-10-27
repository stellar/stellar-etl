package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/stellar/stellar-etl/v2/internal/transform"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

type CloudStorage interface {
	UploadTo(credentialsPath, bucket, path string) error
}

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

func MustOutFile(path string) *os.File {
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		cmdLogger.Fatal("could not get absolute filepath: ", err)
	}

	err = os.MkdirAll(filepath.Dir(path), os.ModePerm)
	if err != nil {
		cmdLogger.Fatalf("could not create directory %s: %s", path, err)
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

// ExportEntry writes entry to outFile as one line of JSON, with the extra
// fields added as top-level keys.
func ExportEntry(entry interface{}, outFile *os.File, extra map[string]string) (int, error) {
	marshalled, err := json.Marshal(entry)
	if err != nil {
		return 0, fmt.Errorf("could not json encode %+v: %s", entry, err)
	}
	marshalled, err = addExtraFields(marshalled, extra)
	if err != nil {
		return 0, fmt.Errorf("could not add extra fields to %+v: %s", entry, err)
	}

	cmdLogger.Debugf("Writing entry to %s", outFile.Name())
	numBytes, err := outFile.Write(append(marshalled, '\n'))
	if err != nil {
		cmdLogger.Errorf("Error writing %+v to file: %s", entry, err)
	}
	return numBytes, nil
}

// addExtraFields splices extra into the JSON object in marshalled without
// decoding it. An extra key that may already be present must override the
// entry's value, so those objects take the slower decode-and-merge path.
func addExtraFields(marshalled []byte, extra map[string]string) ([]byte, error) {
	if len(extra) == 0 {
		return marshalled, nil
	}
	if len(marshalled) < 2 || marshalled[0] != '{' || marshalled[len(marshalled)-1] != '}' {
		return nil, fmt.Errorf("entry is not a JSON object: %s", marshalled)
	}
	for k := range extra {
		quotedKey, err := json.Marshal(k)
		if err != nil {
			return nil, err
		}
		if bytes.Contains(marshalled, append(quotedKey, ':')) {
			return mergeExtraFields(marshalled, extra)
		}
	}

	extraJSON, err := json.Marshal(extra)
	if err != nil {
		return nil, err
	}
	if len(marshalled) == 2 {
		return extraJSON, nil
	}
	// Overwrite the closing brace; marshalled is our own buffer from json.Marshal.
	out := append(marshalled[:len(marshalled)-1], ',')
	return append(out, extraJSON[1:]...), nil
}

// mergeExtraFields decodes marshalled into a map, sets the extra fields over
// it, and re-encodes it.
func mergeExtraFields(marshalled []byte, extra map[string]string) ([]byte, error) {
	i := map[string]interface{}{}
	// UseNumber keeps large ints from being rounded through float64
	decoder := json.NewDecoder(bytes.NewReader(marshalled))
	decoder.UseNumber()
	if err := decoder.Decode(&i); err != nil {
		return nil, err
	}
	for k, v := range extra {
		i[k] = v
	}
	return json.Marshal(i)
}

// Prints the number of attempted, failed, and successful transformations as a JSON object
func PrintTransformStats(attempts, failures int) {
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

func exportParquetFilename(start, end uint32, dataType string) string {
	return fmt.Sprintf("%d-%d-%s.parquet", start, end-1, dataType)
}

func deleteLocalFiles(path string) error {
	err := os.RemoveAll(path)
	if err != nil {
		cmdLogger.Errorf("Unable to remove %s: %s", path, err)
		return err
	}
	cmdLogger.Infof("Successfully deleted %s", path)
	return nil
}

func MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path string) {
	if cloudProvider == "" {
		cmdLogger.Info("No cloud provider specified for upload. Skipping upload.")
		return
	}

	if len(cloudStorageBucket) == 0 {
		cmdLogger.Fatal("No bucket specified")
		return
	}

	var cloudStorage CloudStorage
	switch cloudProvider {
	case "gcp":
		cloudStorage = newGCS(cloudCredentials, cloudStorageBucket)
		err := cloudStorage.UploadTo(cloudCredentials, cloudStorageBucket, path)
		if err != nil {
			cmdLogger.Fatalf("Unable to upload output to GCS: %s", err)
			return
		}
	default:
		cmdLogger.Fatal("Unknown cloud provider")
	}
}

// WriteParquet creates the parquet file and writes the exported data into it.
//
// Parameters:
//
//	data []transform.SchemaParquet  - The slice of data to be written to the Parquet file.
//										SchemaParquet is an interface used to call ToParquet()
//										which is defined for each schema/export.
//	path string                     - The file path where the Parquet file will be created and written.
//										For example, "some/file/path/export_output.parquet"
//	schema interface{}              - The schema that defines the structure of the Parquet file.
//
//	Errors:
//
//	stellar-etl will log a Fatal error and stop in the case it cannot create or write to the parquet file
func WriteParquet(data []transform.SchemaParquet, path string, schema interface{}) {
	parquetFile, err := local.NewLocalFileWriter(path)
	if err != nil {
		cmdLogger.Fatal("could not create parquet file: ", err)
	}
	defer parquetFile.Close()

	writer, err := writer.NewParquetWriter(parquetFile, schema, 1)
	if err != nil {
		cmdLogger.Fatal("could not create parquet file writer: ", err)
	}
	defer writer.WriteStop()

	for _, record := range data {
		if err := writer.Write(record.ToParquet()); err != nil {
			cmdLogger.Fatal("could not write record to parquet file: ", err)
		}
	}
}
