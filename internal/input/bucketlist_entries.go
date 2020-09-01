package input

import (
	"context"

	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/exp/ingest/adapters"
	ingestio "github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/xdr"
)

var archiveStellarURL = "http://history.stellar.org/prd/core-live/core_live_001"

// GetEntriesFromGenesis returns a slice of ledger entries of the specified type for the ledgers starting from the genesis ledger and ending at end (inclusive)
func GetEntriesFromGenesis(end uint32, entryType xdr.LedgerEntryType) ([]ingestio.Change, error) {
	archive, err := historyarchive.Connect(
		archiveStellarURL,
		historyarchive.ConnectOptions{Context: context.Background()},
	)
	if err != nil {
		return []ingestio.Change{}, err
	}

	historyAdapter := adapters.MakeHistoryArchiveAdapter(archive)
	latestNum, err := historyAdapter.GetLatestLedgerSequence()
	if err != nil {
		return []ingestio.Change{}, err
	}

	err = validateLedgerRange(1, end, latestNum)
	if err != nil {
		return []ingestio.Change{}, err
	}

	checkpointSeq, err := utils.GetCheckpointNum(end, latestNum)
	if err != nil {
		return []ingestio.Change{}, err
	}

	return readBucketList(archive, checkpointSeq, entryType)
}

func readBucketList(archive *historyarchive.Archive, checkpointSeq uint32, entryType xdr.LedgerEntryType) ([]ingestio.Change, error) {
	changeReader, err := ingestio.MakeSingleLedgerStateReader(context.Background(), archive, checkpointSeq)
	defer changeReader.Close()
	if err != nil {
		return []ingestio.Change{}, err
	}

	entrySlice := []ingestio.Change{}
	for {
		change, err := changeReader.Read()
		if err == ingestio.EOF {
			break
		}

		if err != nil {
			return []ingestio.Change{}, err
		}

		if change.Type == entryType {
			entrySlice = append(entrySlice, change)
		}
	}

	return entrySlice, nil
}
