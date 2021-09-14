package input

import (
	"context"
	"fmt"
	"io"
	"math"

	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
)

// Mainnet passphrase
const password = network.PublicNetworkPassphrase

// Testnet passphrase
// const password = network.TestNetworkPassphrase

// ChangeBatch represents the changes in a batch of ledgers represented by the range [BatchStart, BatchEnd)
type ChangeBatch struct {
	Changes    []ingest.Change
	BatchStart uint32
	BatchEnd   uint32
	Type       xdr.LedgerEntryType
}

// PrepareCaptiveCore creates a new captive core instance and prepares it with the given range. The range is unbounded when end = 0, and is bounded and validated otherwise
func PrepareCaptiveCore(execPath string, tomlPath string, start, end uint32) (*ledgerbackend.CaptiveStellarCore, error) {
	toml, err := ledgerbackend.NewCaptiveCoreTomlFromFile(
		tomlPath,
		ledgerbackend.CaptiveCoreTomlParams{
			NetworkPassphrase:  password,
			HistoryArchiveURLs: utils.ArchiveURLs,
			Strict:             true,
		},
	)
	if err != nil {
		return &ledgerbackend.CaptiveStellarCore{}, err
	}

	captiveBackend, err := ledgerbackend.NewCaptive(
		ledgerbackend.CaptiveCoreConfig{
			BinaryPath:         execPath,
			Toml:               toml,
			NetworkPassphrase:  password,
			HistoryArchiveURLs: utils.ArchiveURLs,
		},
	)
	if err != nil {
		return &ledgerbackend.CaptiveStellarCore{}, err
	}

	ledgerRange := ledgerbackend.UnboundedRange(start)

	if end != 0 {
		ledgerRange = ledgerbackend.BoundedRange(start, end)
		latest, err := utils.GetLatestLedgerSequence()
		if err != nil {
			return &ledgerbackend.CaptiveStellarCore{}, err
		}

		if err = utils.ValidateLedgerRange(start, end, latest); err != nil {
			return &ledgerbackend.CaptiveStellarCore{}, err
		}
	}

	ctx := context.Background()
	err = captiveBackend.PrepareRange(ctx, ledgerRange)
	if err != nil {
		return &ledgerbackend.CaptiveStellarCore{}, err
	}

	return captiveBackend, nil
}

// sendBatchToChannels sends a ChangeBatch to the appropriate channel, checking that the channel is not nil before sending
func sendBatchToChannels(batch ChangeBatch, accChannel, offChannel, trustChannel chan ChangeBatch) {
	switch batch.Type {
	case xdr.LedgerEntryTypeAccount:
		if accChannel != nil {
			accChannel <- batch
		}

	case xdr.LedgerEntryTypeOffer:
		if offChannel != nil {
			offChannel <- batch
		}

	case xdr.LedgerEntryTypeTrustline:
		if trustChannel != nil {
			trustChannel <- batch
		}

	}
}

// closeChannels checks that the provided channels are not nil, and then closes them
func closeChannels(accChannel, offChannel, trustChannel chan ChangeBatch) {
	if accChannel != nil {
		close(accChannel)
	}

	if offChannel != nil {
		close(offChannel)
	}

	if trustChannel != nil {
		close(trustChannel)
	}
}

func addLedgerChangesToCache(changeReader *ingest.LedgerChangeReader, accCache, offCache, trustCache *ingest.ChangeCompactor) error {
	for {
		change, err := changeReader.Read()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		switch change.Type {
		case xdr.LedgerEntryTypeAccount:
			if accCache != nil {
				accCache.AddChange(change)
			}

		case xdr.LedgerEntryTypeOffer:
			if offCache != nil {
				offCache.AddChange(change)
			}

		case xdr.LedgerEntryTypeTrustline:
			if trustCache != nil {
				trustCache.AddChange(change)
			}

		default:
			// there is also a data entry type, which is not tracked right now
		}
	}
}

// exportBatch gets the changes from the ledgers in the range [batchStart, batchEnd), compacts them, and sends them to the proper channels
func exportBatch(batchStart, batchEnd uint32, core *ledgerbackend.CaptiveStellarCore, accChannel, offChannel, trustChannel chan ChangeBatch, logger *log.Entry) {
	accChanges := ingest.NewChangeCompactor()
	offChanges := ingest.NewChangeCompactor()
	trustChanges := ingest.NewChangeCompactor()
	ctx := context.Background()
	for seq := batchStart; seq < batchEnd; {
		latestLedger, err := core.GetLatestLedgerSequence(ctx)
		if err != nil {
			logger.Fatal("unable to get the lastest ledger sequence: ", err)
		}

		// if this ledger is available, we process its changes and move on to the next ledger by incrementing seq.
		// Otherwise, nothing is incremented and we try again on the next iteration of the loop
		if seq <= latestLedger {
			changeReader, err := ingest.NewLedgerChangeReader(ctx, core, password, seq)
			if err != nil {
				logger.Fatal(fmt.Sprintf("unable to create change reader for ledger %d: ", seq), err)
			}

			err = addLedgerChangesToCache(changeReader, accChanges, offChanges, trustChanges)
			if err != nil {
				logger.Fatal(fmt.Sprintf("unable to read changes from ledger %d: ", seq), err)
			}

			changeReader.Close()
			seq++
		}

	}

	accBatch := ChangeBatch{
		Changes:    accChanges.GetChanges(),
		BatchStart: batchStart,
		BatchEnd:   batchEnd,
		Type:       xdr.LedgerEntryTypeAccount,
	}
	sendBatchToChannels(accBatch, accChannel, nil, nil)

	offBatch := ChangeBatch{
		Changes:    offChanges.GetChanges(),
		BatchStart: batchStart,
		BatchEnd:   batchEnd,
		Type:       xdr.LedgerEntryTypeOffer,
	}
	sendBatchToChannels(offBatch, nil, offChannel, nil)

	trustBatch := ChangeBatch{
		Changes:    trustChanges.GetChanges(),
		BatchStart: batchStart,
		BatchEnd:   batchEnd,
		Type:       xdr.LedgerEntryTypeTrustline,
	}
	sendBatchToChannels(trustBatch, nil, nil, trustChannel)
}

// StreamChanges runs a goroutine that reads in ledgers, processes the changes, and send the changes to the channel matching their type
func StreamChanges(core *ledgerbackend.CaptiveStellarCore, start, end, batchSize uint32, accChannel, offChannel, trustChannel chan ChangeBatch, logger *log.Entry) {
	if end != 0 {
		totalBatches := uint32(math.Ceil(float64(end-start+1) / float64(batchSize)))
		for currentBatch := uint32(0); currentBatch < totalBatches; currentBatch++ {
			batchStart := start + currentBatch*batchSize
			batchEnd := batchStart + batchSize
			if batchEnd > end+1 {
				batchEnd = end + 1
			}

			exportBatch(batchStart, batchEnd, core, accChannel, offChannel, trustChannel, logger)
		}
	} else {
		batchStart := start
		batchEnd := batchStart + batchSize
		for {
			exportBatch(batchStart, batchEnd, core, accChannel, offChannel, trustChannel, logger)
			batchStart = batchEnd
			batchEnd = batchStart + batchSize
		}
	}

	closeChannels(accChannel, offChannel, trustChannel)
}

// ReceiveChanges reads in the ledger entries from the provided channels, transforms them, and adds them to the slice with the other transformed entries.
func ReceiveChanges(accChannel, offChannel, trustChannel chan ChangeBatch, strictExport bool, logger *log.Entry) ([]transform.AccountOutput, []transform.OfferOutput, []transform.TrustlineOutput) {
	transformedAccounts := make([]transform.AccountOutput, 0)
	transformedOffers := make([]transform.OfferOutput, 0)
	transformedTrustlines := make([]transform.TrustlineOutput, 0)
	accBatchRead, offBatchRead, trustBatchRead := false, false, false
	for {
		select {
		case batch, ok := <-accChannel:
			// if ok is false, it means the channel is closed. There will be no more batches, so we can set the channel to nil
			if !ok {
				accChannel = nil
				break
			}

			for _, change := range batch.Changes {
				acc, err := transform.TransformAccount(change)
				if err != nil {
					entry, _, _ := utils.ExtractEntryFromChange(change)
					errorMsg := fmt.Sprintf("error transforming account entry last updated at: %d", entry.LastModifiedLedgerSeq)
					if strictExport {
						logger.Fatal(errorMsg, err)
					} else {
						logger.Warning(errorMsg, err)
						continue
					}
				}

				transformedAccounts = append(transformedAccounts, acc)
			}

			accBatchRead = true

		case batch, ok := <-offChannel:
			if !ok {
				offChannel = nil
				break
			}

			for _, change := range batch.Changes {
				offer, err := transform.TransformOffer(change)
				if err != nil {
					entry, _, _ := utils.ExtractEntryFromChange(change)
					errorMsg := fmt.Sprintf("error transforming offer entry last updated at: %d", entry.LastModifiedLedgerSeq)
					if strictExport {
						logger.Fatal(errorMsg, err)
					} else {
						logger.Warning(errorMsg, err)
						continue
					}
				}

				transformedOffers = append(transformedOffers, offer)
			}

			offBatchRead = true

		case batch, ok := <-trustChannel:
			if !ok {
				trustChannel = nil
				break
			}

			for _, change := range batch.Changes {
				trust, err := transform.TransformTrustline(change)
				if err != nil {
					entry, _, _ := utils.ExtractEntryFromChange(change)
					errorMsg := fmt.Sprintf("error transforming trustline entry last updated at: %d", entry.LastModifiedLedgerSeq)
					if strictExport {
						logger.Fatal(errorMsg, err)
					} else {
						logger.Warning(errorMsg, err)
						continue
					}
				}

				transformedTrustlines = append(transformedTrustlines, trust)
			}

			trustBatchRead = true
		}

		// if a batch has been read from each channel, then break
		if accBatchRead && offBatchRead && trustBatchRead {
			break
		}

		// if the channels are closed, then break
		if accChannel == nil && offChannel == nil && trustChannel == nil {
			break
		}
	}

	return transformedAccounts, transformedOffers, transformedTrustlines
}
