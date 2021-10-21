package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/xdr"
)

// trustlinesCmd represents the trustlines command
var trustlinesCmd = &cobra.Command{
	Use:   "export_trustlines",
	Short: "Exports the trustline data over a specified range.",
	Long: `Exports historical trustline data from the genesis ledger to the provided end-ledger to an output file. 
	The command reads from the bucket list, which includes the full history of the Stellar ledger. As a result, it 
	should be used in an initial data dump. In order to get trustline information within a specified ledger range, see 
	the export_ledger_entry_changes command.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		endNum, useStdout, strictExport, isTest := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		env := utils.GetEnvironmentDetails(isTest)
		path := utils.MustBucketFlags(cmd.Flags(), cmdLogger)

		trustlines, err := input.GetEntriesFromGenesis(endNum, xdr.LedgerEntryTypeTrustline, env.ArchiveURLs)
		if err != nil {
			cmdLogger.Fatal("could not read trustlines: ", err)
		}

		var outFile *os.File
		if !useStdout {
			outFile = mustOutFile(path)
		}

		failures := 0
		for _, trust := range trustlines {
			transformed, err := transform.TransformTrustline(trust)
			if err != nil {
				if strictExport {
					cmdLogger.Fatal("could not transform trustline", err)
				} else {
					cmdLogger.Warning("could not transform trustline", err)
					failures++
					continue
				}
			}

			marshalled, err := json.Marshal(transformed)
			if err != nil {
				if strictExport {
					cmdLogger.Fatal("could not json encode trustline", err)
				} else {
					cmdLogger.Warning("could not json encode trustline", err)
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
			printLog := true
			if !useStdout {
				printLog = false
			}
			printTransformStats(len(trustlines), failures, printLog)
		}
	},
}

func init() {
	rootCmd.AddCommand(trustlinesCmd)
	utils.AddCommonFlags(trustlinesCmd.Flags())
	utils.AddBucketFlags("trustlines", trustlinesCmd.Flags())
	trustlinesCmd.MarkFlagRequired("end-ledger")

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
