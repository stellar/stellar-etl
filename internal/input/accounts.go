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

// GetAccountsFromGenesis returns a slice of ledger close metas for the ledgers starting from the genesis ledger and ending at end (inclusive)
func GetAccountsFromGenesis(end uint32) ([]xdr.LedgerEntry, error) {
	archive, err := historyarchive.Connect(
		archiveStellarURL,
		historyarchive.ConnectOptions{Context: context.Background()},
	)
	if err != nil {
		return []xdr.LedgerEntry{}, err
	}

	historyAdapter := adapters.MakeHistoryArchiveAdapter(archive)
	latestNum, err := historyAdapter.GetLatestLedgerSequence()
	if err != nil {
		return []xdr.LedgerEntry{}, err
	}

	err = validateLedgerRange(1, end, latestNum)
	if err != nil {
		return []xdr.LedgerEntry{}, err
	}

	checkpointSeq, err := utils.GetCheckpointNum(end, latestNum)
	if err != nil {
		return []xdr.LedgerEntry{}, err
	}

	return readBucketList(archive, checkpointSeq, xdr.LedgerEntryTypeAccount)
}

func readBucketList(archive *historyarchive.Archive, checkpointSeq uint32, entryType xdr.LedgerEntryType) ([]xdr.LedgerEntry, error) {
	changeReader, err := ingestio.MakeSingleLedgerStateReader(context.Background(), archive, checkpointSeq)
	defer changeReader.Close()
	if err != nil {
		return []xdr.LedgerEntry{}, err
	}

	entrySlice := []xdr.LedgerEntry{}
	for {
		change, err := changeReader.Read()
		if err == ingestio.EOF {
			break
		}

		if err != nil {
			return []xdr.LedgerEntry{}, err
		}

		if change.Type == entryType {
			entrySlice = append(entrySlice, *change.Post)
		}
	}

	return entrySlice, nil
}
