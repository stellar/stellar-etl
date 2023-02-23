package cmd

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stellar/stellar-etl/internal/utils"
	"github.com/stretchr/testify/assert"
)

var executableName = "stellar-etl"
var latestLedger, _ = utils.GetLatestLedgerSequenceFromGCSBackend(utils.BUCKET_POC)
var update = flag.Bool("update", false, "update the golden files of this test")
var gotFolder = "testdata/got/"

type cliTest struct {
	name              string
	args              []string
	golden            string
	wantErr           error
	sortForComparison bool
}

func TestMain(m *testing.M) {
	if err := os.Chdir(".."); err != nil {
		cmdLogger.Error("could not change directory", err)
		os.Exit(1)
	}

	// This does the setup for further tests. It generates an executeable that can be run on the command line by other tests
	buildCmd := exec.Command("go", "build", "-o", executableName)
	if err := buildCmd.Run(); err != nil {
		cmdLogger.Error("could not build executable", err)
		os.Exit(1)
	}

	flag.Parse()
	exitCode := m.Run()
	os.Exit(exitCode)
}

func gotTestDir(t *testing.T, filename string) string {
	return filepath.Join(gotFolder, t.Name(), filename)
}

func TestExportLedger(t *testing.T) {
	tests := []cliTest{
		{
			name:    "end before start",
			args:    []string{"export_ledgers", "--testnet", "-s", "100", "-e", "50"},
			golden:  "",
			wantErr: fmt.Errorf("-end-ledger must be >= -start-ledger"),
		},
		{
			name:    "start too large",
			args:    []string{"export_ledgers", "--testnet", "-s", "4294967295", "-e", "4294967295"},
			golden:  "",
			wantErr: fmt.Errorf("latest sequence number is less than start sequence number (%d < 4294967295)", latestLedger),
		},
		{
			name:    "end too large",
			args:    []string{"export_ledgers", "--testnet", "-e", "4294967295", "-l", "4294967295"},
			golden:  "",
			wantErr: fmt.Errorf("-start-ledger must be >= 2"),
		},
		{
			name:    "start is 0",
			args:    []string{"export_ledgers", "--testnet", "-s", "0", "-e", "4294967295", "-l", "4294967295"},
			golden:  "",
			wantErr: fmt.Errorf("-start-ledger must be >= 2"),
		},
		{
			name:    "end is 0",
			args:    []string{"export_ledgers", "--testnet", "-e", "0", "-l", "4294967295"},
			golden:  "",
			wantErr: fmt.Errorf("-start-ledger must be >= 2"),
		},
		{
			name:    "single ledger",
			args:    []string{"export_ledgers", "--testnet", "--gcs-bucket", "not", "-s", "1410100", "-e", "1410100", "-o", gotTestDir(t, "single_ledger.txt")},
			golden:  "single_ledger.golden",
			wantErr: nil,
		},
		{
			name:    "10 ledgers",
			args:    []string{"export_ledgers", "--testnet", "--gcs-bucket", "not", "-s", "1410100", "-e", "1410110", "-o", gotTestDir(t, "10_ledgers.txt")},
			golden:  "10_ledgers.golden",
			wantErr: nil,
		},
		{
			name:    "range too large",
			args:    []string{"export_ledgers", "--testnet", "--gcs-bucket", "not", "-s", "1410100", "-e", "1410125", "-l", "5", "-o", gotTestDir(t, "large_range_ledgers.txt")},
			golden:  "large_range_ledgers.golden",
			wantErr: nil,
		},
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/ledgers/")
	}
}

func indexOf(l []string, s string) int {
	for idx, e := range l {
		if e == s {
			return idx
		}
	}
	return -1
}

func sortByName(files []os.DirEntry) {
	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() < files[j].Name()
	})
}

func runCLITest(t *testing.T, test cliTest, goldenFolder string) {
	t.Run(test.name, func(t *testing.T) {
		dir, err := os.Getwd()
		assert.NoError(t, err)

		cmd := exec.Command(path.Join(dir, executableName), test.args...)
		errOut, actualError := cmd.CombinedOutput()

		idxOfOutputArg := indexOf(test.args, "-o")
		testOutput := []byte{}
		if idxOfOutputArg > -1 {
			outLocation := test.args[idxOfOutputArg+1]
			stat, err := os.Stat(outLocation)
			assert.NoError(t, err)

			// If the output arg specified is a directory, concat the contents for comparison.
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
		if test.golden == "" {
			errorMsg := fmt.Errorf(extractErrorMsg(string(errOut)))
			assert.Equal(t, test.wantErr, errorMsg)
			return
		}

		assert.Equal(t, test.wantErr, actualError)
		actualString := string(testOutput)
		if test.sortForComparison {
			trimmed := strings.Trim(actualString, "\n")
			lines := strings.Split(trimmed, "\n")
			sort.Strings(lines)
			actualString = strings.Join(lines, "\n")
			actualString = fmt.Sprintf("%s\n", actualString)
		}

		wantString, err := getGolden(t, goldenFolder+test.golden, actualString, *update)
		assert.NoError(t, err)
		assert.Equal(t, wantString, actualString)
	})
}

func extractErrorMsg(loggerOutput string) string {
	errIndex := strings.Index(loggerOutput, "msg=") + 5
	endIndex := strings.Index(loggerOutput[errIndex:], "\"")
	return loggerOutput[errIndex : errIndex+endIndex]
}

func getGolden(t *testing.T, goldenFile string, actual string, update bool) (string, error) {
	update = true
	t.Helper()
	f, err := os.OpenFile(goldenFile, os.O_RDWR, 0644)
	defer func() {
		err = f.Close()
	}()
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

	wantOutput, err := io.ReadAll(f)
	if err != nil {
		return "", err
	}

	return string(wantOutput), nil
}
