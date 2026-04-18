package input

import (
	"context"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/support/compressxdr"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-etl/v2/internal/utils"
)

// GetLedgerRange converts a time range to a ledger range by binary-searching the GCS datastore
// configured on env.CommonFlagValues.DatastorePath. Each probe fetches a single
// LedgerCloseMetaBatch file via the datastore's random-access API, so it does not rely on the
// sequential BufferedStorageBackend path used by the bulk export commands.
func GetLedgerRange(startTime, endTime time.Time, env utils.EnvironmentDetails) (int64, int64, error) {
	startTime = startTime.UTC()
	endTime = endTime.UTC()
	if startTime.After(endTime) {
		return 0, 0, fmt.Errorf("start time must be less than or equal to the end time")
	}

	ctx := context.Background()
	ds, dsCfg, err := utils.CreateDatastore(ctx, env)
	if err != nil {
		return 0, 0, err
	}
	defer ds.Close()

	finder := &ledgerFinder{
		ds:     ds,
		schema: dsCfg.Schema,
		cache:  map[uint32]ledgerPoint{},
	}

	oldestSeq, err := datastore.FindOldestLedgerSequence(ctx, ds, dsCfg.Schema)
	if err != nil {
		return 0, 0, fmt.Errorf("unable to find oldest ledger in datastore: %w", err)
	}
	latestSeq, err := datastore.FindLatestLedgerSequence(ctx, ds)
	if err != nil {
		return 0, 0, fmt.Errorf("unable to find latest ledger in datastore: %w", err)
	}

	oldestPt, err := finder.pointAt(ctx, oldestSeq)
	if err != nil {
		return 0, 0, err
	}
	latestPt, err := finder.pointAt(ctx, latestSeq)
	if err != nil {
		return 0, 0, err
	}

	// Clamp requested times to the datastore's available range (same semantics as the
	// history-archive-backed implementation's limitLedgerRange).
	if startTime.Before(oldestPt.closeTime) {
		startTime = oldestPt.closeTime
	} else if startTime.After(latestPt.closeTime) {
		startTime = latestPt.closeTime
	}
	if endTime.After(latestPt.closeTime) {
		endTime = latestPt.closeTime
	} else if endTime.Before(oldestPt.closeTime) {
		endTime = oldestPt.closeTime
	}

	startLedger, err := finder.findLedgerForTime(ctx, startTime, oldestPt, latestPt)
	if err != nil {
		return 0, 0, err
	}
	endLedger, err := finder.findLedgerForTime(ctx, endTime, oldestPt, latestPt)
	if err != nil {
		return 0, 0, err
	}

	return int64(startLedger), int64(endLedger), nil
}

type ledgerPoint struct {
	seq       uint32
	closeTime time.Time
}

type ledgerFinder struct {
	ds     datastore.DataStore
	schema datastore.DataStoreSchema
	cache  map[uint32]ledgerPoint
}

// pointAt fetches the close time for a single ledger sequence by downloading its
// LedgerCloseMetaBatch file from the datastore and decoding the contained LedgerCloseMeta.
// Results are memoized to avoid duplicate GETs across the two binary searches and between
// findLedgerForTime's neighbour probes.
func (f *ledgerFinder) pointAt(ctx context.Context, seq uint32) (ledgerPoint, error) {
	if pt, ok := f.cache[seq]; ok {
		return pt, nil
	}
	key := f.schema.GetObjectKeyFromSequenceNumber(seq)
	rc, err := f.ds.GetFile(ctx, key)
	if err != nil {
		return ledgerPoint{}, fmt.Errorf("unable to fetch ledger %d (%s) from datastore: %w", seq, key, err)
	}
	defer rc.Close()

	var batch xdr.LedgerCloseMetaBatch
	dec := compressxdr.NewXDRDecoder(compressxdr.DefaultCompressor, &batch)
	if _, err := dec.ReadFrom(rc); err != nil {
		return ledgerPoint{}, fmt.Errorf("unable to decode batch for ledger %d: %w", seq, err)
	}

	lcm, err := batch.GetLedger(seq)
	if err != nil {
		return ledgerPoint{}, fmt.Errorf("batch does not contain ledger %d: %w", seq, err)
	}
	closeTime, err := utils.GetCloseTime(lcm)
	if err != nil {
		return ledgerPoint{}, fmt.Errorf("unable to extract close time for ledger %d: %w", seq, err)
	}

	pt := ledgerPoint{seq: seq, closeTime: closeTime}
	f.cache[seq] = pt
	return pt, nil
}

// findLedgerForTime returns the first ledger whose close time is >= target, assuming
// start.seq <= end.seq and close times are monotonically non-decreasing. The boundary
// predicate (prev.Unix() < target && curr.Unix() >= target) matches the existing
// findLedgerForTimeBinary in the history-archive implementation, so golden outputs align.
func (f *ledgerFinder) findLedgerForTime(ctx context.Context, target time.Time, start, end ledgerPoint) (uint32, error) {
	if start.seq >= end.seq {
		return start.seq, nil
	}

	mid := start.seq + (end.seq-start.seq)/2
	midPt, err := f.pointAt(ctx, mid)
	if err != nil {
		return 0, err
	}

	if mid > start.seq {
		prevPt, err := f.pointAt(ctx, mid-1)
		if err != nil {
			return 0, err
		}
		if prevPt.closeTime.Unix() < target.Unix() && midPt.closeTime.Unix() >= target.Unix() {
			return mid, nil
		}
	}

	if midPt.closeTime.Unix() > target.Unix() {
		newEnd, err := f.pointAt(ctx, mid-1)
		if err != nil {
			return 0, err
		}
		return f.findLedgerForTime(ctx, target, start, newEnd)
	}

	newStart, err := f.pointAt(ctx, mid+1)
	if err != nil {
		return 0, err
	}
	return f.findLedgerForTime(ctx, target, newStart, end)
}
