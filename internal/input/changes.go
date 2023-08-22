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

var (
	ExtractBatch = extractBatch
)

// ChangeBatch represents the changes in a batch of ledgers represented by the range [BatchStart, BatchEnd)
type ChangeBatch struct {
	Changes    map[xdr.LedgerEntryType][]ChangeWithLedgerHeader
	BatchStart uint32
	BatchEnd   uint32
}

type ChangeWithLedgerHeader struct {
	Change ingest.Change
	Header xdr.LedgerHeaderHistoryEntry
}

// PrepareCaptiveCore creates a new captive core instance and prepares it with the given range. The range is unbounded when end = 0, and is bounded and validated otherwise
func PrepareCaptiveCore(execPath string, tomlPath string, start, end uint32, env utils.EnvironmentDetails) (*ledgerbackend.CaptiveStellarCore, error) {
	toml, err := ledgerbackend.NewCaptiveCoreTomlFromFile(
		tomlPath,
		ledgerbackend.CaptiveCoreTomlParams{
			NetworkPassphrase:  env.NetworkPassphrase,
			HistoryArchiveURLs: env.ArchiveURLs,
			Strict:             true,
			UseDB:              true,
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
			UseDB:              true,
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

// extractBatch gets the changes from the ledgers in the range [batchStart, batchEnd] and compacts them
func extractBatch(
	batchStart, batchEnd uint32,
	core *ledgerbackend.CaptiveStellarCore,
	env utils.EnvironmentDetails, logger *utils.EtlLogger) ChangeBatch {

	changes := map[xdr.LedgerEntryType][]ChangeWithLedgerHeader{}
	ctx := context.Background()
	for seq := batchStart; seq <= batchEnd; {
		latestLedger, err := core.GetLatestLedgerSequence(ctx)
		if err != nil {
			logger.Fatal("unable to get the latest ledger sequence: ", err)
		}

		// if this ledger is available, we process its changes and move on to the next ledger by incrementing seq.
		// Otherwise, nothing is incremented, and we try again on the next iteration of the loop
		if seq <= latestLedger {
			changeReader, err := ingest.NewLedgerChangeReader(ctx, core, env.NetworkPassphrase, seq)
			if err != nil {
				logger.Fatal(fmt.Sprintf("unable to create change reader for ledger %d: ", seq), err)
			}
			// TODO: Add in ledger_closed_at; Update changeCompactors to also save ledger close time.
			//   AddChange is from the go monorepo so it might be easier to just add a addledgerclose func after it
			//txReader := changeReader.LedgerTransactionReader

			//closeTime, err := utils.TimePointToUTCTimeStamp(txReader.GetHeader().Header.ScpValue.CloseTime)
			//if err != nil {
			//	logger.Fatal(fmt.Sprintf("unable to read close time for ledger %d: ", seq), err)
			//}
			ledgerHeader := changeReader.LedgerTransactionReader.GetHeader()

			for {
				change, err := changeReader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					logger.Fatal(fmt.Sprintf("unable to read changes from ledger %d: ", seq), err)
				}

				changes[change.Type] = append(changes[change.Type], ChangeWithLedgerHeader{change, ledgerHeader})
			}

			changeReader.Close()
			seq++
		}
	}

	return ChangeBatch{
		Changes:    changes,
		BatchStart: batchStart,
		BatchEnd:   batchEnd,
	}
}

// StreamChanges reads in ledgers, processes the changes, and send the changes to the channel matching their type
// Ledgers are processed in batches of size <batchSize>.
func StreamChanges(core *ledgerbackend.CaptiveStellarCore, start, end, batchSize uint32, changeChannel chan ChangeBatch, closeChan chan int, env utils.EnvironmentDetails, logger *utils.EtlLogger) {
	batchStart := start
	batchEnd := uint32(math.Min(float64(batchStart+batchSize), float64(end)))
	for batchStart < batchEnd {
		if batchEnd < end {
			batchEnd = uint32(batchEnd - 1)
		}
		batch := ExtractBatch(batchStart, batchEnd, core, env, logger)
		changeChannel <- batch
		// batchStart and batchEnd should not overlap
		// overlapping batches causes duplicate record loads
		batchStart = uint32(math.Min(float64(batchEnd), float64(end)) + 1)
		batchEnd = uint32(math.Min(float64(batchStart+batchSize), float64(end)))
	}
	close(changeChannel)
	closeChan <- 1
}
