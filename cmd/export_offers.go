package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"
)

// offersCmd represents the offers command
var offersCmd = &cobra.Command{
	Use:   "export_offers",
	Short: "Exports the data on offers made from the genesis ledger to a specified endpoint.",
	Long: `Exports historical offer data from the genesis ledger to the provided end-ledger to an output file. 
	The command reads from the bucket list, which includes the full history of the Stellar ledger. As a result, it 
	should be used in an initial data dump. In order to get offer information within a specified ledger range, see 
	the export_ledger_entry_changes command.`,
	Run: func(cmd *cobra.Command, args []string) {
		endNum, path, useStdout := utils.MustBucketFlags(cmd.Flags(), cmdLogger)

		var outFile *os.File
		if !useStdout {
			outFile = mustOutFile(path)
		}

		offers, err := input.GetEntriesFromGenesis(endNum, xdr.LedgerEntryTypeOffer)
		if err != nil {
			cmdLogger.Fatal("could not read offers: ", err)
		}

		for _, offer := range offers {
			transformed, err := transform.TransformOffer(offer)
			if err != nil {
				cmdLogger.Fatal("could not transform offer", err)
			}

			marshalled, err := json.Marshal(transformed)
			if err != nil {
				cmdLogger.Fatal("could not json encode offer", err)
			}

			if !useStdout {
				outFile.Write(marshalled)
				outFile.WriteString("\n")
			} else {
				fmt.Println(string(marshalled))
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(offersCmd)
	utils.AddBucketFlags("offers", offersCmd.Flags())
	offersCmd.MarkFlagRequired("end-ledger")
	/*
		Current flags:
			end-ledger: the ledger sequence number for the end of the export range (required)
			output-file: filename of the output file
			stdout: if set, output is printed to stdout

		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			end time as a replacement for end sequence numbers
	*/
}
