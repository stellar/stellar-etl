package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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

func mustOutFile(path string) *os.File {
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

func exportEntry(entry interface{}, outFile *os.File, extra map[string]string) (int, error) {
	// This extra marshalling/unmarshalling is silly, but it's required to properly handle the null.[String|Int*] types, and add the extra fields.
	m, err := json.Marshal(entry)
	if err != nil {
		cmdLogger.Errorf("Error marshalling %+v: %v ", entry, err)
	}
	i := map[string]interface{}{}
	// Use a decoder here so that 'UseNumber' ensures large ints are properly decoded
	decoder := json.NewDecoder(bytes.NewReader(m))
	decoder.UseNumber()
	err = decoder.Decode(&i)
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
	cmdLogger.Debugf("Writing entry to %s", outFile.Name())
	numBytes, err := outFile.Write(marshalled)
	if err != nil {
		cmdLogger.Errorf("Error writing %+v to file: %s", entry, err)
	}
	newLineNumBytes, err := outFile.WriteString("\n")
	if err != nil {
		cmdLogger.Errorf("Error writing new line to file %s: %s", outFile.Name(), err)
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

func deleteLocalFiles(path string) {
	err := os.RemoveAll(path)
	if err != nil {
		cmdLogger.Errorf("Unable to remove %s: %s", path, err)
		return
	}
	cmdLogger.Infof("Successfully deleted %s", path)
}

func maybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path string) {
	if cloudProvider == "" {
		cmdLogger.Info("No cloud provider specified for upload. Skipping upload.")
		return
	}

	if len(cloudStorageBucket) == 0 {
		cmdLogger.Error("No bucket specified")
		return
	}

	var cloudStorage CloudStorage
	switch cloudProvider {
	case "gcp":
		cloudStorage = newGCS(cloudCredentials, cloudStorageBucket)
		err := cloudStorage.UploadTo(cloudCredentials, cloudStorageBucket, path)
		if err != nil {
			cmdLogger.Errorf("Unable to upload output to GCS: %s", err)
			return
		}
	default:
		cmdLogger.Error("Unknown cloud provider")
	}
}
