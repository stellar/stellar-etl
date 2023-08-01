package input

import (
	"context"
	"io"

	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-etl/internal/utils"
)

// GetEntriesFromGenesis returns a slice of ledger entries of the specified type for the ledgers starting from the genesis ledger and ending at end (inclusive)
func GetEntriesFromGenesis(end uint32, entryType xdr.LedgerEntryType, archiveURLs []string) ([]ingest.Change, error) {
	archive, err := utils.CreateHistoryArchiveClient(archiveURLs)
	if err != nil {
		return []ingest.Change{}, err
	}

	latestNum, err := utils.GetLatestLedgerSequence(archiveURLs)
	if err != nil {
		return []ingest.Change{}, err
	}

	if err = utils.ValidateLedgerRange(1, end, latestNum); err != nil {
		return []ingest.Change{}, err
	}

	checkpointSeq, err := utils.GetCheckpointNum(end, latestNum)
	if err != nil {
		return []ingest.Change{}, err
	}

	return readBucketList(archive, checkpointSeq, entryType)
}

func readBucketList(archive historyarchive.ArchiveInterface, checkpointSeq uint32, entryType xdr.LedgerEntryType) ([]ingest.Change, error) {
	changeReader, err := ingest.NewCheckpointChangeReader(context.Background(), archive, checkpointSeq)
	defer changeReader.Close()
	if err != nil {
		return []ingest.Change{}, err
	}

	entrySlice := []ingest.Change{}
	for {
		change, err := changeReader.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			return []ingest.Change{}, err
		}

		if change.Type == entryType {
			entrySlice = append(entrySlice, change)
		}
	}

	return entrySlice, nil
}

func GetLedgerCloseMeta(end uint32, env utils.EnvironmentDetails) (xdr.LedgerCloseMeta, error) {
	ctx := context.Background()
	captiveCoreToml, err := ledgerbackend.NewCaptiveCoreTomlFromFile(
		env.CoreConfig,
		ledgerbackend.CaptiveCoreTomlParams{
			NetworkPassphrase:  env.NetworkPassphrase,
			HistoryArchiveURLs: env.ArchiveURLs,
			Strict:             true,
		},
	)
	if err != nil {
		return xdr.LedgerCloseMeta{}, err
	}
	backend, err := ledgerbackend.NewCaptive(
		ledgerbackend.CaptiveCoreConfig{
			BinaryPath:         env.BinaryPath,
			Toml:               captiveCoreToml,
			NetworkPassphrase:  env.NetworkPassphrase,
			HistoryArchiveURLs: env.ArchiveURLs,
		},
	)

	ledgerCloseMeta, err := backend.GetLedger(ctx, end)
	if err != nil {
		return xdr.LedgerCloseMeta{}, errors.Wrap(err, "error getting ledger from the backend")
	}

	return ledgerCloseMeta, nil
}
