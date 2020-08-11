package cmd

import (
	"fmt"
	"path/filepath"

	"github.com/spf13/cobra"
	ingestio "github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"
)

var exportLedgerEntryChangesCmd = &cobra.Command{
	Use:   "export_ledger_entry_changes",
	Short: "This command exports the changes in accounts, offers, and trustlines.",
	Long: `This command instantiates a stellar-core instance and uses it to export about accounts, offers, and trustlines.
The information is exported in batches determined by the batch-size flag. Each exported file will include the changes to the 
relevent data type that occurred during that batch.

If the end-ledger is omitted, then the stellar-core node will continue running and exporting information as new ledgers are 
confirmed by the Stellar network. 

If no data type flags are set, then by default all of them are exported. If any are set, it is assumed that the others should not
be exported.`,
	Run: func(cmd *cobra.Command, args []string) {
		startNum, endNum, _, _, _ := utils.MustBasicFlags(cmd.Flags(), cmdLogger)
		execPath, configPath, exportAccounts, exportOffers, exportTrustlines, _ := utils.MustCoreFlags(cmd.Flags(), cmdLogger)

		if configPath == "" && endNum == 0 {
			cmdLogger.Fatal("A path to a config file for stellar-core is mandatory when performing an unbounded export")
		}

		var err error
		execPath, err = filepath.Abs(execPath)
		if err != nil {
			cmdLogger.Fatal("could not get absolute filepath for stellar-core executable: ", err)
		}

		configPath, err = filepath.Abs(configPath)
		if err != nil {
			cmdLogger.Fatal("could not get absolute filepath for the config file: ", err)
		}

		/*var outFile *os.File
		if !useStdout {
			outFile = mustOutFile(path)
		}*/

		core, err := input.PrepareCaptiveCore(execPath, configPath, startNum, endNum)
		if err != nil {
			cmdLogger.Fatal("error creating a prepared captive core instance: ", err)
		}

		accChannel, offChannel, trustChannel := createChangeChannels(exportAccounts, exportOffers, exportTrustlines)
		go input.StreamChanges(core, startNum, endNum, accChannel, offChannel, trustChannel)

		transformedAccounts := make([]transform.AccountOutput, 0)
		transformedOffers := make([]transform.OfferOutput, 0)
		transformedTrustlines := make([]transform.TrustlineOutput, 0)

		for {

			select {
			case entry, ok := <-accChannel:
				if !ok {
					accChannel = nil
				} else {
					acc, err := transform.TransformAccount(entry)
					if err == nil {
						transformedAccounts = append(transformedAccounts, acc)
					} else {
						cmdLogger.Error("error transforming account entry: ", err)
					}
				}

			case entry, ok := <-offChannel:
				wrappedEntry := ingestio.Change{Type: xdr.LedgerEntryTypeOffer, Post: &entry}
				offer, err := transform.TransformOffer(wrappedEntry)
				if !ok {
					offChannel = nil
				} else {
					if err == nil {
						transformedOffers = append(transformedOffers, offer)
					} else {
						cmdLogger.Error("error transforming offer entry: ", err)
					}
				}

			case entry, ok := <-trustChannel:
				trust, err := transform.TransformTrustline(entry)
				if !ok {
					trustChannel = nil
				} else {
					if err == nil {
						transformedTrustlines = append(transformedTrustlines, trust)
					} else {
						cmdLogger.Error("error transforming trustline entry: ", err)
					}
				}
			}

			if accChannel == nil && offChannel == nil && trustChannel == nil {
				break
			}
		}

		fmt.Println(transformedAccounts)
		fmt.Println(transformedOffers)
		fmt.Println(transformedTrustlines)
		/*
			1. Instantiate a captive core instance
				a) If the start and end are provided, then use a bounded range and exit after exporting the info inside the range
				b) If the end is omitted, use an unbounded range and continue exporting as new ledgers are added to the network
			2. Call GetLedger() constantly in a separate goroutine
				a) Create channels for each data type
				b) Process changes for the ledger and send changes to the channel matching their type
			3. On the other end, receive changes from the channel
				a) Call transform on individual changes
				b) Once batch_size ledgers have been sent, encode and export the changes
		*/
	},
}

func createChangeChannels(exportAccounts, exportOffers, exportTrustlines bool) (accChan, offChan, trustChan chan xdr.LedgerEntry) {
	if !exportAccounts && !exportOffers && !exportTrustlines {
		accChan = make(chan xdr.LedgerEntry)
		offChan = make(chan xdr.LedgerEntry)
		trustChan = make(chan xdr.LedgerEntry)
		return
	}

	if exportAccounts {
		accChan = make(chan xdr.LedgerEntry)
	}

	if exportOffers {
		offChan = make(chan xdr.LedgerEntry)
	}

	if exportTrustlines {
		trustChan = make(chan xdr.LedgerEntry)
	}

	return
}

func init() {
	rootCmd.AddCommand(exportLedgerEntryChangesCmd)
	utils.AddBasicFlags("changes", exportLedgerEntryChangesCmd.Flags())
	utils.AddCoreFlags(exportLedgerEntryChangesCmd.Flags())
	exportLedgerEntryChangesCmd.MarkFlagRequired("start-ledger")
	exportLedgerEntryChangesCmd.MarkFlagRequired("core-executable")
	/*
		Current flags:
			start-ledger: the ledger sequence number for the beginning of the export period
			end-ledger: the ledger sequence number for the end of the export range

			output-file: filename of the output file
			stdout: if true, prints to stdout instead of the command line
			limit: maximum number of changes to export in a given batch; if negative then everything gets exported
			batch-size: size of the export batches

			core-executable: path to stellar-core executable
			core-config: path to stellar-core config file

			If none of the export_X flags are set, assume everything should be exported
				export_accounts: boolean flag; if set then accounts should be exported
				export_trustlines: boolean flag; if set then trustlines should be exported
				export_offers: boolean flag; if set then offers should be exported

		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			start and end time as a replacement for start and end sequence numbers
	*/
}
