package input

import (
	"context"
	"fmt"
	"io"

	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/exp/ingest/adapters"
	ingestio "github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/xdr"
)

// GetAccounts returns a slice of ledger close metas for the ledgers in the provided range (inclusive on both ends)
func GetAccounts(start, end uint32, limit int64) ([]xdr.LedgerEntry, error) {
	archiveStellarURL := "http://history.stellar.org/prd/core-live/core_live_001"
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

	err = validateLedgerRange(start, end, latestNum)
	if err != nil {
		return []xdr.LedgerEntry{}, err
	}

	// If the start is set to the genesis ledger, everything is read from the bucket list up to the nearest checkpoint. The limit flag is ignored
	if start == 1 {
		checkpointSeq, err := utils.GetCheckpointNum(end, latestNum)
		if err != nil {
			return []xdr.LedgerEntry{}, err
		}

		fmt.Printf("Using checkpoint: %d\n", checkpointSeq)
		return readBucketList(archive, checkpointSeq, xdr.LedgerEntryTypeAccount)
	}

	// Use captive core to read a range otherwise
	return readFromCaptive(start, end)
}

func readFromCaptive(start, end uint32) ([]xdr.LedgerEntry, error) {
	return []xdr.LedgerEntry{}, nil
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
		if err == io.EOF {
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
