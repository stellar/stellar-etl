package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/transform"
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

// ledgersCmd represents the ledgers command
var ledgersCmd = &cobra.Command{
	Use:   "export_ledgers",
	Short: "Exports the ledger data.",
	Long:  `Exports ledger data within the specified range to an output file. Data is appended to the output file after being encoded as a JSON object.`,
	Run: func(cmd *cobra.Command, args []string) {
		startNum, err := cmd.Flags().GetUint32("start-ledger")
		if err != nil {
			logger.Fatal("could not get start sequence number: ", err)
		}

		endNum, err := cmd.Flags().GetUint32("end-ledger")
		if err != nil {
			logger.Fatal("could not get end sequence number: ", err)
		}

		limit, err := cmd.Flags().GetUint32("limit")
		if err != nil {
			logger.Fatal("could not get limit: ", err)
		}

		path, err := cmd.Flags().GetString("output-file")
		if err != nil {
			logger.Fatal("could not get output filename: ", err)
		}

		absolutePath, err := filepath.Abs(path)
		if err != nil {
			logger.Fatal("could not get absolute filepath: ", err)
		}

		err = createOutputFile(absolutePath)
		if err != nil {
			logger.Fatal("could not create output file: ", err)
		}

		// TODO: check the permissions of the file to ensure that it can be written to
		outFile, err := os.OpenFile(absolutePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			logger.Fatal("could not create output file: ", err)
		}

		ledgers, err := input.GetLedgers(startNum, endNum, limit)
		if err != nil {
			logger.Fatal("could not read ledgers: ", err)
		}

		for i, lcm := range ledgers {
			transformed, err := transform.TransformLedger(lcm)
			if err != nil {
				logger.Fatal(fmt.Sprintf("could not transform ledger %d: ", startNum+uint32(i)), err)
			}

			marshalled, err := json.Marshal(transformed)
			if err != nil {
				logger.Fatal(fmt.Sprintf("could not json encode ledger %d: ", startNum+uint32(i)), err)
			}

			outFile.Write(marshalled)
			outFile.WriteString("\n")
		}
	},
}

func init() {
	rootCmd.AddCommand(ledgersCmd)
	ledgersCmd.Flags().Uint32P("start-ledger", "s", 0, "The ledger sequence number for the beginning of the export period")
	ledgersCmd.Flags().Uint32P("end-ledger", "e", 0, "The ledger sequence number for the end of the export range (required)")
	ledgersCmd.Flags().Uint32P("limit", "l", 60, "Maximum number of ledgers to export")
	ledgersCmd.Flags().StringP("output-file", "o", "exported_ledgers.txt", "Filename of the output file")
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
