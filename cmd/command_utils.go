package cmd

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stretchr/testify/assert"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

var update = flag.Bool("update", false, "update the Golden files of this test")
var gotFolder = "testdata/got/"

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

func ExportEntry(entry interface{}, outFile *os.File, extra map[string]string) (int, error) {
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

type CliTest struct {
	Name              string
	Args              []string
	Golden            string
	WantErr           error
	SortForComparison bool
}

func indexOf(l []string, s string) int {
	for idx, e := range l {
		if e == s {
			return idx
		}
	}
	return -1
}

func RunCLITest(t *testing.T, test CliTest, GoldenFolder string, executableName string, useParentDir bool) {
	if executableName == "" {
		executableName = "stellar-etl"
	}
	flag.Parse()
	t.Run(test.Name, func(t *testing.T) {
		dir, err := os.Getwd()
		assert.NoError(t, err)

		if useParentDir {
			dir = filepath.Dir(dir)
		}

		idxOfOutputArg := indexOf(test.Args, "-o")
		var testOutput []byte
		var outLocation string
		var stat os.FileInfo
		if idxOfOutputArg > -1 {
			outLocation = test.Args[idxOfOutputArg+1]
			_, err = os.Stat(outLocation)
			if err != nil {
				// Check if the error is due to the file not existing
				if !os.IsNotExist(err) {
					assert.NoError(t, err)
				}
			} else {
				err = deleteLocalFiles(outLocation)
				if err != nil {
					log.Fatal(err)
				}
			}
		}

		cmd := exec.Command(executableName, test.Args...)
		fmt.Printf("Command: %s %v\n", executableName, test.Args)
		errOut, actualError := cmd.CombinedOutput()
		if idxOfOutputArg > -1 {
			stat, err = os.Stat(outLocation)
			assert.NoError(t, err)

			if stat.IsDir() {
				files, err := os.ReadDir(outLocation)
				if err != nil {
					log.Fatal(err)
				}
				var buf bytes.Buffer
				sortByName(files)
				for _, f := range files {
					b, err := os.ReadFile(filepath.Join(outLocation, f.Name()))
					if err != nil {
						log.Fatal(err)
					}
					buf.Write(b)
				}
				testOutput = buf.Bytes()
			} else {
				// If the output is written to a file, read the contents of the file for comparison.
				testOutput, err = os.ReadFile(outLocation)
				if err != nil {
					log.Fatal(err)
				}
			}
		}

		// Since the CLI uses a logger to report errors, the final error message isn't the same as the errors thrown in code.
		// Instead, it's wrapped in other os/system errors
		// By reading the error text from the logger, we can extract the lower level error that the user would see
		if test.Golden == "" {
			errorMsg := fmt.Errorf(extractErrorMsg(string(errOut)))
			assert.Equal(t, test.WantErr, errorMsg)
			return
		}

		assert.Equal(t, test.WantErr, actualError)
		actualString := string(testOutput)
		if test.SortForComparison {
			trimmed := strings.Trim(actualString, "\n")
			lines := strings.Split(trimmed, "\n")
			sort.Strings(lines)
			actualString = strings.Join(lines, "\n")
			actualString = fmt.Sprintf("%s\n", actualString)
		}

		wantString, err := getGolden(t, GoldenFolder+test.Golden, actualString, *update)
		assert.NoError(t, err)
		assert.Equal(t, wantString, actualString)
	})
}

func extractErrorMsg(loggerOutput string) string {
	errIndex := strings.Index(loggerOutput, "msg=") + 5
	endIndex := strings.Index(loggerOutput[errIndex:], "\"")
	return loggerOutput[errIndex : errIndex+endIndex]
}

func getGolden(t *testing.T, GoldenFile string, actual string, update bool) (string, error) {
	t.Helper()
	f, err := os.OpenFile(GoldenFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return "", err
	}
	defer f.Close()

	// If the update flag is true, clear the current contents of the Golden file and write the actual output
	// This is useful for when new tests or added or functionality changes that breaks current tests
	if update {
		err := os.Truncate(GoldenFile, 0)
		if err != nil {
			return "", err
		}

		_, err = f.WriteString(actual)
		if err != nil {
			return "", err
		}
		return actual, nil
	}

	wantOutput, err := io.ReadAll(f)
	if err != nil {
		return "", err
	}

	return string(wantOutput), nil
}

func sortByName(files []os.DirEntry) {
	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() < files[j].Name()
	})
}

func GotTestDir(t *testing.T, filename string) string {
	return filepath.Join(gotFolder, t.Name(), filename)
}
