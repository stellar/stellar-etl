package cmd

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/stellar/stellar-etl/internal/input"

	"github.com/spf13/cobra"
)

type ledgerRange struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

var getLedgerRangeFromTimesCmd = &cobra.Command{
	Use:   "get_ledger_range_from_times",
	Short: "Converts a time range into a ledger range",
	Long: `Converts a time range into a ledger range. Times must be in the format YYYY-MM-DDTHH:MM:SS.SSSZ.

	Some examples include: 2006-01-02T15:04:05-07:00, 2009-11-10T18:00:00-05:00, or 2019-09-13T23:00:00+00:00.
	If the time range goes into the future, the ledger range will end on the most recent ledger. If the time
	range covers time before the network started, the ledger range will start with the genesis ledger.`,
	Run: func(cmd *cobra.Command, args []string) {
		startString, err := cmd.Flags().GetString("start-time")
		if err != nil {
			cmdLogger.Fatal("could not get start time: ", err)
		}

		endString, err := cmd.Flags().GetString("end-time")
		if err != nil {
			cmdLogger.Fatal("could not get end time: ", err)
		}

		path, err := cmd.Flags().GetString("output")
		if err != nil {
			cmdLogger.Fatal("could not get output path: ", err)
		}

		isTest, err := cmd.Flags().GetBool("testnet")
		if err != nil {
			cmdLogger.Fatal("could not get testnet boolean: ", err)
		}

		formatString := "2006-01-02T15:04:05-07:00"
		startTime, err := time.Parse(formatString, startString)
		if err != nil {
			cmdLogger.Fatal("could not parse start time: ", err)
		}

		endTime, err := time.Parse(formatString, endString)
		if err != nil {
			cmdLogger.Fatal("could not parse end time: ", err)
		}

		startLedger, endLedger, err := input.GetLedgerRange(startTime, endTime, isTest)
		if err != nil {
			cmdLogger.Fatal("could not calculate ledger range: ", err)
		}

		toExport := ledgerRange{Start: startLedger, End: endLedger}
		marshalled, err := json.Marshal(toExport)
		if err != nil {
			cmdLogger.Fatal("could not json encode ledger range", err)
		}

		if path != "" {
			outFile := mustOutFile(path)
			outFile.Write(marshalled)
			outFile.WriteString("\n")
			outFile.Close()
		} else {
			fmt.Println(string(marshalled))
		}
	},
}

func init() {
	rootCmd.AddCommand(getLedgerRangeFromTimesCmd)

	getLedgerRangeFromTimesCmd.Flags().StringP("start-time", "s", "", "The start time")
	getLedgerRangeFromTimesCmd.Flags().StringP("end-time", "e", "", "The end time")
	getLedgerRangeFromTimesCmd.Flags().StringP("output", "o", "exported_range.txt", "Filename of the output file")
	getLedgerRangeFromTimesCmd.Flags().Bool("testnet", false, "If set, the batch job will connect to testnet instead of mainnet.")

	getLedgerRangeFromTimesCmd.MarkFlagRequired("start-time")
	getLedgerRangeFromTimesCmd.MarkFlagRequired("end-time")
}
