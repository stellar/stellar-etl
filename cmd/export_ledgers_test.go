package cmd

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"

	"github.com/stellar/go/exp/ingest/ledgerbackend"
	"github.com/stretchr/testify/assert"
)

var executableName = "stellar-etl"
var archiveURL = "http://history.stellar.org/prd/core-live/core_live_001"
var latestLedger = getLastSeqNum()
var update = flag.Bool("update", false, "update the golden files of this test")
var backend, _ = ledgerbackend.NewHistoryArchiveBackendFromURL(archiveURL)

func TestMain(m *testing.M) {
	if err := os.Chdir(".."); err != nil {
		logger.Error("could not change directory", err)
		os.Exit(1)
	}

	// This does the setup for further tests. It generates an executeable that can be run on the command line by other tests
	buildCmd := exec.Command("go", "build", "-o", executableName)
	if err := buildCmd.Run(); err != nil {
		logger.Error("could not build executable", err)
		os.Exit(1)
	}

	flag.Parse()
	exitCode := m.Run()
	backend.Close()
	os.Exit(exitCode)
}

func TestTransformLedger(t *testing.T) {
	type cliTest struct {
		name    string
		args    []string
		golden  string
		wantErr error
	}
	var testFileName = "test.txt"

	tests := []cliTest{
		{
			name:    "end before start",
			args:    []string{"export_ledgers", "-s", "100", "-e", "50", "-o", testFileName},
			golden:  "",
			wantErr: fmt.Errorf("could not read ledgers: End sequence number is less than start (50 < 100)"),
		},
		{
			name:    "range too large",
			args:    []string{"export_ledgers", "-s", "0", "-e", "100", "-l", "10", "-o", testFileName},
			golden:  "",
			wantErr: fmt.Errorf("could not read ledgers: Range of [0, 100] is too large for limit of 10"),
		},
		{
			name:    "start too large",
			args:    []string{"export_ledgers", "-s", "4294967295", "-e", "4294967295", "-o", testFileName},
			golden:  "",
			wantErr: fmt.Errorf("could not read ledgers: Latest sequence number is less than start sequence number (%d < 4294967295)", latestLedger),
		},
		{
			name:    "end too large",
			args:    []string{"export_ledgers", "-e", "4294967295", "-l", "4294967295", "-o", testFileName},
			golden:  "",
			wantErr: fmt.Errorf("could not read ledgers: Latest sequence number is less than end sequence number (%d < 4294967295)", latestLedger),
		},
		{
			name:    "single ledger",
			args:    []string{"export_ledgers", "-s", "30822015", "-e", "30822015", "-o", testFileName},
			golden:  "single_ledger.golden",
			wantErr: nil,
		},
		{
			name:    "10 ledgers",
			args:    []string{"export_ledgers", "-s", "30822015", "-e", "30822025", "-o", testFileName},
			golden:  "10_ledgers.golden",
			wantErr: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir, err := os.Getwd()
			assert.NoError(t, err)

			cmd := exec.Command(path.Join(dir, executableName), test.args...)
			errText, actualError := cmd.CombinedOutput()

			// Since the CLI uses a logger to report errors, the final error message isn't the same as the errors thrown in code.
			// Instead, it's wrapped in other os/system errors
			// By reading the error text from the logger, we can extract the lower level error that the user would see
			if test.golden == "" {
				errorMsg := fmt.Errorf(extractErrorMsg(string(errText)))
				assert.Equal(t, test.wantErr, errorMsg)
			}

			// Tests that are designed to fail won't generate an valid output file, and so testing shouldn't look for one
			if test.golden != "" {
				assert.Equal(t, test.wantErr, actualError)
				actualOutput, err := ioutil.ReadFile(testFileName)
				assert.NoError(t, err)
				actualString := string(actualOutput)

				wantString, err := getGolden(t, "testdata/"+test.golden, actualString, *update)
				assert.NoError(t, err)
				assert.Equal(t, wantString, actualString)
			}
			os.Remove(testFileName)
		})
	}
}

func extractErrorMsg(loggerOutput string) string {
	errIndex := strings.Index(loggerOutput, "msg=") + 5
	endIndex := strings.Index(loggerOutput[errIndex:], "\"")
	return loggerOutput[errIndex : errIndex+endIndex]
}

func getLastSeqNum() uint32 {
	num, _ := backend.GetLatestLedgerSequence()
	return num
}

func getGolden(t *testing.T, goldenFile string, actual string, update bool) (string, error) {
	t.Helper()
	f, err := os.OpenFile(goldenFile, os.O_RDWR, 0644)
	defer f.Close()
	if err != nil {
		return "", err
	}

	// If the update flag is true, clear the current contents of the golden file and write the actual output
	// This is useful for when new tests or added or functionality changes that breaks current tests
	if update {
		err := os.Truncate(goldenFile, 0)
		if err != nil {
			return "", err
		}

		_, err = f.WriteString(actual)
		if err != nil {
			return "", err
		}

		return actual, nil
	}

	wantOutput, err := ioutil.ReadAll(f)
	if err != nil {
		return "", err
	}

	return string(wantOutput), nil
}