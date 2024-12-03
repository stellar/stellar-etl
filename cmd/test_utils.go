package cmd

import (
	"bytes"
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

	"github.com/stretchr/testify/assert"
)

var update = flag.Bool("update", false, "update the Golden files of this test")
var gotFolder = "testdata/got/"

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
