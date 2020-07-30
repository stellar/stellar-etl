package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
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

func getBasicFlags(flags *pflag.FlagSet) (startNum, endNum uint32, limit int64, path string, useStdOut bool) {
	startNum, err := flags.GetUint32("start-ledger")
	if err != nil {
		logger.Fatal("could not get start sequence number: ", err)
	}

	endNum, err = flags.GetUint32("end-ledger")
	if err != nil {
		logger.Fatal("could not get end sequence number: ", err)
	}

	limit, err = flags.GetInt64("limit")
	if err != nil {
		logger.Fatal("could not get limit: ", err)
	}

	path, err = flags.GetString("output")
	if err != nil {
		logger.Fatal("could not get output filename: ", err)
	}

	useStdOut, err = flags.GetBool("stdout")
	if err != nil {
		logger.Fatal("could not get stdout boolean: ", err)
	}

	return
}

func getOutFile(path string) *os.File {
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
		logger.Fatal("error in output file: ", err)
	}

	return outFile
}

func addBasicFlags(objectName string, flags *pflag.FlagSet) {
	flags.Uint32P("start-ledger", "s", 0, "The ledger sequence number for the beginning of the export period")
	flags.Uint32P("end-ledger", "e", 0, "The ledger sequence number for the end of the export range (required)")
	flags.Int64P("limit", "l", -1, "Maximum number of "+objectName+" to export. If the limit is set to a negative number, all the objects in the provided range are exported")
	flags.StringP("output", "o", "exported_"+objectName+".txt", "Filename of the output file")
	flags.Bool("stdout", false, "If set, the output will be printed to stdout instead of to a file")
}

var ledgersCmd = &cobra.Command{
	Use:   "export_ledgers",
	Short: "Exports the ledger data.",
	Long:  `Exports ledger data within the specified range to an output file. Data is appended to the output file after being encoded as a JSON object.`,
	Run: func(cmd *cobra.Command, args []string) {
		startNum, endNum, limit, path, useStdOut := getBasicFlags(cmd.Flags())

		var outFile *os.File
		if !useStdOut {
			outFile = getOutFile(path)
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

			if !useStdOut {
				outFile.Write(marshalled)
				outFile.WriteString("\n")
			} else {
				fmt.Println(string(marshalled))
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(ledgersCmd)
	addBasicFlags("ledgers", ledgersCmd.Flags())
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
