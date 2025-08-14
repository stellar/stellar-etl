package input

import (
	"context"
	"fmt"
	"io"
	"math"

	"github.com/stellar/stellar-etl/v2/internal/utils"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/xdr"
)

var (
	ExtractBatch = extractBatch
)

type LedgerChanges struct {
	Changes       []ingest.Change
	LedgerHeaders []xdr.LedgerHeaderHistoryEntry
}

// ChangeBatch represents the changes in a batch of ledgers represented by the range [BatchStart, BatchEnd)
type ChangeBatch struct {
	Changes    map[xdr.LedgerEntryType]LedgerChanges
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
			UserAgent:          "stellar-etl/1.0.0",
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
	backend *ledgerbackend.LedgerBackend,
	env utils.EnvironmentDetails, logger *utils.EtlLogger) ChangeBatch {

	dataTypes := []xdr.LedgerEntryType{
		xdr.LedgerEntryTypeAccount,
		xdr.LedgerEntryTypeOffer,
		xdr.LedgerEntryTypeTrustline,
		xdr.LedgerEntryTypeLiquidityPool,
		xdr.LedgerEntryTypeClaimableBalance,
		xdr.LedgerEntryTypeContractData,
		xdr.LedgerEntryTypeContractCode,
		xdr.LedgerEntryTypeConfigSetting,
		xdr.LedgerEntryTypeTtl}

	ledgerChanges := map[xdr.LedgerEntryType]LedgerChanges{}
	ctx := context.Background()
	for seq := batchStart; seq <= batchEnd; {
		changeCompactors := map[xdr.LedgerEntryType]*ingest.ChangeCompactor{}
		for _, dt := range dataTypes {
			changeCompactors[dt] = ingest.NewChangeCompactor(ingest.ChangeCompactorConfig{SuppressRemoveAfterRestoreChange: false})
		}

		// if this ledger is available, we process its changes and move on to the next ledger by incrementing seq.
		// Otherwise, nothing is incremented, and we try again on the next iteration of the loop
		var header xdr.LedgerHeaderHistoryEntry
		if seq <= batchEnd {
			changeReader, err := ingest.NewLedgerChangeReader(ctx, *backend, env.NetworkPassphrase, seq)
			if err != nil {
				logger.Fatal(fmt.Sprintf("unable to create change reader for ledger %d: ", seq), err)
			}
			header = changeReader.LedgerTransactionReader.GetHeader()

			for {
				change, err := changeReader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					logger.Fatal(fmt.Sprintf("unable to read changes from ledger %d: ", seq), err)
				}
				cache, ok := changeCompactors[change.Type]
				if !ok {
					// TODO: once LedgerEntryTypeData is tracked as well, all types should be addressed,
					// so this info log should be a warning.
					// Skip LedgerEntryTypeData as we are intentionally not processing it
					if change.Type != xdr.LedgerEntryTypeData {
						logger.Infof("change type: %v not tracked", change.Type)
					}
				} else {
					cache.AddChange(change)
				}
			}

			changeReader.Close()
			seq++
		}

		for dataType, compactor := range changeCompactors {
			for _, change := range compactor.GetChanges() {
				dataTypeChanges := ledgerChanges[dataType]
				dataTypeChanges.Changes = append(dataTypeChanges.Changes, change)
				dataTypeChanges.LedgerHeaders = append(dataTypeChanges.LedgerHeaders, header)
				ledgerChanges[dataType] = dataTypeChanges
			}
		}

	}

	return ChangeBatch{
		Changes:    ledgerChanges,
		BatchStart: batchStart,
		BatchEnd:   batchEnd,
	}
}

// StreamChanges reads in ledgers, processes the changes, and send the changes to the channel matching their type
// Ledgers are processed in batches of size <batchSize>.
func StreamChanges(backend *ledgerbackend.LedgerBackend, start, end, batchSize uint32, changeChannel chan ChangeBatch, closeChan chan int, env utils.EnvironmentDetails, logger *utils.EtlLogger) {
	batchStart := start
	batchEnd := uint32(math.Min(float64(batchStart+batchSize), float64(end)))
	for batchStart < batchEnd {
		if batchEnd < end {
			batchEnd = uint32(batchEnd - 1)
		}
		batch := ExtractBatch(batchStart, batchEnd, backend, env, logger)
		changeChannel <- batch
		// batchStart and batchEnd should not overlap
		// overlapping batches causes duplicate record loads
		batchStart = uint32(math.Min(float64(batchEnd), float64(end)) + 1)
		batchEnd = uint32(math.Min(float64(batchStart+batchSize), float64(end)))
	}
	close(changeChannel)
	closeChan <- 1
}
