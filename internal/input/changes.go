package input

import (
	"context"
	"fmt"
	"io"
	"math"

	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/xdr"
)

// ChangeBatch represents the changes in a batch of ledgers represented by the range [BatchStart, BatchEnd)
type ChangeBatch struct {
	Changes    map[xdr.LedgerEntryType][]ingest.Change
	BatchStart uint32
	BatchEnd   uint32
}

// PrepareCaptiveCore creates a new captive core instance and prepares it with the given range. The range is unbounded when end = 0, and is bounded and validated otherwise
func PrepareCaptiveCore(execPath string, tomlPath string, start, end uint32, env utils.EnvironmentDetails) (*ledgerbackend.CaptiveStellarCore, error) {
	toml, err := ledgerbackend.NewCaptiveCoreTomlFromFile(
		tomlPath,
		ledgerbackend.CaptiveCoreTomlParams{
			NetworkPassphrase:  env.NetworkPassphrase,
			HistoryArchiveURLs: env.ArchiveURLs,
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
			NetworkPassphrase:  env.NetworkPassphrase,
			HistoryArchiveURLs: env.ArchiveURLs,
		},
	)
	if err != nil {
		return &ledgerbackend.CaptiveStellarCore{}, err
	}

	ledgerRange := ledgerbackend.UnboundedRange(start)

	if end != 0 {
		ledgerRange = ledgerbackend.BoundedRange(start, end)
		latest, err := utils.GetLatestLedgerSequence(env.ArchiveURLs)
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

// exportBatch gets the changes from the ledgers in the range [batchStart, batchEnd], compacts them, and sends them to the proper channels
func exportBatch(batchStart, batchEnd uint32, core *ledgerbackend.CaptiveStellarCore, changeChannel chan ChangeBatch, env utils.EnvironmentDetails, logger *utils.EtlLogger) {
	accChanges := ingest.NewChangeCompactor()
	offChanges := ingest.NewChangeCompactor()
	trustChanges := ingest.NewChangeCompactor()
	ctx := context.Background()
	for seq := batchStart; seq <= batchEnd; {
		latestLedger, err := core.GetLatestLedgerSequence(ctx)
		if err != nil {
			logger.Fatal("unable to get the lastest ledger sequence: ", err)
		}

		// if this ledger is available, we process its changes and move on to the next ledger by incrementing seq.
		// Otherwise, nothing is incremented and we try again on the next iteration of the loop
		if seq <= latestLedger {
			changeReader, err := ingest.NewLedgerChangeReader(ctx, core, env.NetworkPassphrase, seq)
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

	batch := ChangeBatch{
		Changes: map[xdr.LedgerEntryType][]ingest.Change{
			xdr.LedgerEntryTypeAccount:   accChanges.GetChanges(),
			xdr.LedgerEntryTypeOffer:     offChanges.GetChanges(),
			xdr.LedgerEntryTypeTrustline: trustChanges.GetChanges(),
		},
		BatchStart: batchStart,
		BatchEnd:   batchEnd,
	}

	changeChannel <- batch
}

// StreamChanges reads in ledgers, processes the changes, and send the changes to the channel matching their type
// Ledgers are processed in batches of size <batchSize>.
func StreamChanges(core *ledgerbackend.CaptiveStellarCore, start, end, batchSize uint32, changeChannel chan ChangeBatch, closeChan chan int, env utils.EnvironmentDetails, logger *utils.EtlLogger) {
	batchStart := start
	batchEnd := uint32(math.Min(float64(batchStart+batchSize), float64(end)))
	for batchStart < batchEnd {
		exportBatch(batchStart, batchEnd, core, changeChannel, env, logger)
		batchStart = uint32(math.Min(float64(batchEnd), float64(end)))
		batchEnd = uint32(math.Min(float64(batchStart+batchSize), float64(end)))
	}
	close(changeChannel)
	closeChan <- 1
}
