package input

import (
	"fmt"

	ingestio "github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/exp/ingest/ledgerbackend"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"
)

func getLatestLedgerNumber() (uint32, error) {
	backend, err := utils.CreateBackend()
	if err != nil {
		return 0, err
	}

	defer backend.Close()

	latestNum, err := backend.GetLatestLedgerSequence()
	if err != nil {
		return 0, err
	}

	return latestNum, nil
}

// PrepareCaptiveCore creates a new captive core instance and prepares it with the given range. The range is unbounded when end = 0, and is bounded and validated otherwise
func PrepareCaptiveCore(execPath, configPath string, start, end uint32) (*ledgerbackend.CaptiveStellarCore, error) {
	captiveBackend, err := ledgerbackend.NewCaptive(
		execPath,
		configPath,
		network.PublicNetworkPassphrase,
		[]string{archiveStellarURL},
	)
	if err != nil {
		return &ledgerbackend.CaptiveStellarCore{}, err
	}

	ledgerRange := ledgerbackend.UnboundedRange(start)

	if end != 0 {
		ledgerRange = ledgerbackend.BoundedRange(start, end)
		latest, err := getLatestLedgerNumber()
		if err != nil {
			return &ledgerbackend.CaptiveStellarCore{}, err
		}

		err = validateLedgerRange(start, end, latest)
		if err != nil {
			return &ledgerbackend.CaptiveStellarCore{}, err
		}
	}

	err = captiveBackend.PrepareRange(ledgerRange)
	if err != nil {
		return &ledgerbackend.CaptiveStellarCore{}, err
	}

	return captiveBackend, nil
}

// getCompactedChanges compacts the read changes from a ledger and returns them
func getCompactedChanges(changeReader *ingestio.LedgerChangeReader) []ingestio.Change {
	changeCache := ingestio.NewLedgerEntryChangeCache()
	for {
		change, err := changeReader.Read()
		if err == ingestio.EOF {
			break
		}

		if err == nil {
			changeCache.AddChange(change)
		}
	}

	return changeCache.GetChanges()
}

// sendToChannel sends a ledgerentry to the appropriate channel, checking that the channel is not nil before sending
func sendToChannel(entry xdr.LedgerEntry, accChannel, offChannel, trustChannel chan xdr.LedgerEntry) {
	switch entry.Data.Type {
	case xdr.LedgerEntryTypeAccount:
		if accChannel != nil {
			accChannel <- entry
		}

	case xdr.LedgerEntryTypeOffer:
		if offChannel != nil {
			offChannel <- entry
		}

	case xdr.LedgerEntryTypeTrustline:
		if trustChannel != nil {
			trustChannel <- entry
		}

	}
}

// closeChannels checks that the provided channels are not nil, and then closes them
func closeChannels(accChannel, offChannel, trustChannel chan xdr.LedgerEntry) {
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

// sendLedgerChangesToChannels streams the changes from a single ledger to the provided channels
func sendLedgerChangesToChannels(seqNum uint32, core *ledgerbackend.CaptiveStellarCore, accChannel, offChannel, trustChannel chan xdr.LedgerEntry) {
	changeReader, err := ingestio.NewLedgerChangeReader(core, network.PublicNetworkPassphrase, seqNum)
	if err == nil {
		compactedChanges := getCompactedChanges(changeReader)
		for _, change := range compactedChanges {
			// TODO: figure out how to handle deleted entries
			if change.Post != nil {
				entry := *change.Post
				sendToChannel(entry, accChannel, offChannel, trustChannel)
			}
		}
	}
}

// StreamChanges runs a goroutine that reads in ledgers, processes the changes, and send the changes to the channel matching their type
func StreamChanges(core *ledgerbackend.CaptiveStellarCore, start, end uint32, accChannel, offChannel, trustChannel chan xdr.LedgerEntry) {
	if end != 0 {
		for seq := start; seq <= end; seq++ {
			sendLedgerChangesToChannels(seq, core, accChannel, offChannel, trustChannel)
		}
	} else {
		currentLedger := start
		for {
			sendLedgerChangesToChannels(currentLedger, core, accChannel, offChannel, trustChannel)
			currentLedger++
		}
	}

	closeChannels(accChannel, offChannel, trustChannel)
}

// ReceiveChanges reads in the ledger entries from the provided channels, transforms them, and adds them to the slice with the other transformed entries.
func ReceiveChanges(accChannel, offChannel, trustChannel chan xdr.LedgerEntry, logger *log.Entry) ([]transform.AccountOutput, []transform.OfferOutput, []transform.TrustlineOutput) {
	transformedAccounts := make([]transform.AccountOutput, 0)
	transformedOffers := make([]transform.OfferOutput, 0)
	transformedTrustlines := make([]transform.TrustlineOutput, 0)

	for {

		select {
		case entry, ok := <-accChannel:
			if !ok {
				accChannel = nil
				break
			}

			acc, err := transform.TransformAccount(entry)
			if err != nil {
				errorMsg := fmt.Sprintf("error transforming account entry last updated at: %d", entry.LastModifiedLedgerSeq)
				logger.Error(errorMsg, err)
				break
			}

			transformedAccounts = append(transformedAccounts, acc)

		case entry, ok := <-offChannel:
			if !ok {
				offChannel = nil
				break
			}

			wrappedEntry := ingestio.Change{Type: xdr.LedgerEntryTypeOffer, Post: &entry}
			offer, err := transform.TransformOffer(wrappedEntry)
			if err != nil {
				errorMsg := fmt.Sprintf("error transforming offer entry last updated at: %d", entry.LastModifiedLedgerSeq)
				logger.Error(errorMsg, err)
				break
			}

			transformedOffers = append(transformedOffers, offer)

		case entry, ok := <-trustChannel:
			if !ok {
				trustChannel = nil
				break
			}

			trust, err := transform.TransformTrustline(entry)
			if err != nil {
				errorMsg := fmt.Sprintf("error transforming trustline entry last updated at: %d", entry.LastModifiedLedgerSeq)
				logger.Error(errorMsg, err)
				break
			}

			transformedTrustlines = append(transformedTrustlines, trust)
		}

		if accChannel == nil && offChannel == nil && trustChannel == nil {
			break
		}
	}
	return transformedAccounts, transformedOffers, transformedTrustlines
}
