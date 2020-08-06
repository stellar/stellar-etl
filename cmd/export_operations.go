package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"
)

var operationsCmd = &cobra.Command{
	Use:   "export_operations",
	Short: "Exports the operations data over a specified range",
	Long:  `Exports the operations data over a specified range. Each operation is an individual command that mutates the Stellar ledger.`,
	Run: func(cmd *cobra.Command, args []string) {
		startNum, endNum, limit, path, useStdout := utils.MustBasicFlags(cmd.Flags(), cmdLogger)

		var outFile *os.File
		if !useStdout {
			outFile = mustOutFile(path)
		}

		operations, err := input.GetOperations(startNum, endNum, limit)
		if err != nil {
			cmdLogger.Fatal("could not read operations: ", err)
		}

		for _, transformInput := range operations {
			transformed, err := transform.TransformOperation(transformInput.Operation, transformInput.OperationIndex, transformInput.Transaction)
			if err != nil {
				txIndex := transformInput.Transaction.Index
				errMsg := fmt.Sprintf("could not transform operation %d in transaction %d: ", transformInput.OperationIndex, txIndex)
				cmdLogger.Fatal(errMsg, err)
			}

			marshalled, err := json.Marshal(transformed)
			if err != nil {
				txIndex := transformInput.Transaction.Index
				errMsg := fmt.Sprintf("could not json encode operation %d in ledger %d: ", transformInput.OperationIndex, txIndex)
				cmdLogger.Fatal(errMsg, err)
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
	rootCmd.AddCommand(operationsCmd)
	utils.AddBasicFlags("operations", operationsCmd.Flags())
	operationsCmd.MarkFlagRequired("end-ledger")

	/*
		Current flags:
			start-ledger: the ledger sequence number for the beginning of the export period
			end-ledger: the ledger sequence number for the end of the export range (required)

			limit: maximum number of operations to export; default to 6,000,000
				each transaction can have up to 100 operations
				each ledger can have up to 1000 transactions
				there are 60 new ledgers in a 5 minute period

			output-file: filename of the output file

		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			start and end time as a replacement for start and end sequence numbers
	*/
}
