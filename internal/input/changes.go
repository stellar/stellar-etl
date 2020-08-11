package input

import (
	ingestio "github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/exp/ingest/ledgerbackend"
	"github.com/stellar/go/network"
	"github.com/stellar/go/xdr"
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

// StreamChanges reads in ledgers, processes the changes, and send the changes to the channel matching their type
func StreamChanges(core *ledgerbackend.CaptiveStellarCore, start, end uint32, accChannel, offChannel, trustChannel chan xdr.LedgerEntry) {
	if end != 0 {
		for seq := start; seq <= end; seq++ {
			changeReader, err := ingestio.NewLedgerChangeReader(core, network.PublicNetworkPassphrase, seq)
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
	} else {
		currentLedger := start
		for {
			changeReader, err := ingestio.NewLedgerChangeReader(core, network.PublicNetworkPassphrase, currentLedger)
			if err == nil {
				compactedChanges := getCompactedChanges(changeReader)
				for _, change := range compactedChanges {
					// TODO: figure out how to handle deleted entries
					if change.Post != nil {
						entry := *change.Post
						sendToChannel(entry, accChannel, offChannel, trustChannel)
					}
				}

				currentLedger++
			}
		}
	}

	closeChannels(accChannel, offChannel, trustChannel)
}
