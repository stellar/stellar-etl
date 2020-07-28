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
			logger.Fatal("could not get start sequence number:", err)
		}

		endNum, err := cmd.Flags().GetUint32("end-ledger")
		if err != nil {
			logger.Fatal("could not get start sequence number:", err)
		}

		limit, err := cmd.Flags().GetInt("limit")
		if err != nil {
			logger.Fatal("could not get start sequence number:", err)
		}

		if endNum-startNum+1 > uint32(limit) {
			logger.Fatal("limit too small for range:", fmt.Errorf("Range from %d to %d is too large for limit of %d", startNum, endNum, limit))
		}

		path, err := cmd.Flags().GetString("output-file")
		if err != nil {
			logger.Fatal("could not get filepath:", err)
		}

		absolutePath, err := filepath.Abs(path)
		if err != nil {
			logger.Fatal("could not get absolute filepath:", err)
		}

		err = createOutputFile(absolutePath)
		if err != nil {
			logger.Fatal("could not create output file:", err)
		}

		outFile, err := os.OpenFile(absolutePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			logger.Fatal("could not create output file", err)
		}

		ledgers, err := input.GetLedgers(startNum, endNum)
		if err != nil {
			logger.Fatal("could not read ledgers", err)
		}

		for i, lcm := range ledgers {
			transformed, err := transform.TransformLedger(lcm)
			if err != nil {
				logger.Fatal(fmt.Sprintf("could not transform ledger %d", startNum+uint32(i)), err)
			}

			marshalled, err := json.Marshal(transformed)
			if err != nil {
				logger.Fatal(fmt.Sprintf("could not json encode ledger %d", startNum+uint32(i)), err)
			}

			outFile.Write(marshalled)
		}

		/*
			Functionality planning:
			1. Read in start and end ledger numbers/timestamps
				1b. If timestamps are received, convert them to ledger sequence numbers
			2. Get each ledger in the range from the ingestion system
			3. For each ledger received, make a corresponding Ledger struct in the output slice (slice has a max length of limit)
			4. Serialize slice and output it to a file
		*/
	},
}

func init() {
	rootCmd.AddCommand(ledgersCmd)
	ledgersCmd.Flags().Uint32P("start-ledger", "s", 0, "The ledger sequence number for the beginning of the export period")
	ledgersCmd.Flags().Uint32P("end-ledger", "e", 0, "The ledger sequence number for the end of the export range (required)")
	ledgersCmd.Flags().IntP("limit", "l", 60, "Maximum number of ledgers to export")
	ledgersCmd.Flags().StringP("output-file", "o", "exported_ledgers.txt", "Filename of the output file")
	ledgersCmd.MarkFlagRequired("end-ledger")
	/*
		Needed flags:
			TODO: determine if providing ledger sequence number or timestamp is preferable (possibly could do both)
				If we do both, do not require both end-time and end-ledger

			start-time: the time for the beginning of the period to export; default to genesis ledger's creation time
			end-time: the time for the end of the period to export (required)

			start-ledger: the ledger sequence number for the beginning of the export period
			end-ledger: the ledger sequence number for the end of the export range (required)

			limit: maximum number of ledgers to export; default to 60 (1 ledger per 5 seconds over our 5 minute update period)
			output-file: filename of the output file

		Extra flags that may be useful:
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
	*/
}
