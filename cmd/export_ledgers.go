package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"
)

func createOutputFile(filepath string) error {
	var _, err = os.Stat(filepath)
	if os.IsNotExist(err) {
		var file, err = os.Create(filepath)
		if err != nil {
			return err
		}

		defer file.Close()
	}

	return nil
}

func mustOutFile(path string) *os.File {
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		cmdLogger.Fatal("could not get absolute filepath: ", err)
	}

	err = createOutputFile(absolutePath)
	if err != nil {
		cmdLogger.Fatal("could not create output file: ", err)
	}

	outFile, err := os.OpenFile(absolutePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		cmdLogger.Fatal("error in opening output file: ", err)
	}

	return outFile
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
		cmdLogger.Fatal("Could not marshall results: ", err)
	}

	fmt.Println(string(results))
}

var ledgersCmd = &cobra.Command{
	Use:   "export_ledgers",
	Short: "Exports the ledger data.",
	Long:  `Exports ledger data within the specified range to an output file. Data is appended to the output file after being encoded as a JSON object.`,
	Run: func(cmd *cobra.Command, args []string) {
		endNum, useStdout, strictExport := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		startNum, path, limit := utils.MustArchiveFlags(cmd.Flags(), cmdLogger)

		var outFile *os.File
		if !useStdout {
			outFile = mustOutFile(path)
		}

		ledgers, err := input.GetLedgers(startNum, endNum, limit)
		if err != nil {
			cmdLogger.Fatal("could not read ledgers: ", err)
		}

		failures := 0
		for i, lcm := range ledgers {
			transformed, err := transform.TransformLedger(lcm)
			if err != nil {
				errMsg := fmt.Sprintf("could not transform ledger %d: ", startNum+uint32(i))
				if strictExport {
					cmdLogger.Fatal(errMsg, err)
				} else {
					cmdLogger.Warning(errMsg, err)
					failures++
					continue
				}
			}

			marshalled, err := json.Marshal(transformed)
			if err != nil {
				errMsg := fmt.Sprintf("could not json encode ledger %d: ", startNum+uint32(i))
				if strictExport {
					cmdLogger.Fatal(errMsg, err)
				} else {
					cmdLogger.Warning(errMsg, err)
					failures++
					continue
				}
			}

			if !useStdout {
				outFile.Write(marshalled)
				outFile.WriteString("\n")
			} else {
				fmt.Println(string(marshalled))
			}
		}

		if !strictExport {
			printTransformStats(len(ledgers), failures)
		}
	},
}

func init() {
	rootCmd.AddCommand(ledgersCmd)
	utils.AddCommonFlags(ledgersCmd.Flags())
	utils.AddArchiveFlags("ledgers", ledgersCmd.Flags())
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
