package input

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/support/compressxdr"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// testSchema mirrors the "one ledger per file, one file per partition" layout so that each
// sequence maps to a distinct object key and we can target mock expectations precisely.
var testSchema = datastore.DataStoreSchema{LedgersPerFile: 1, FilesPerPartition: 1}

// encodedBatch returns a zstd-compressed LedgerCloseMetaBatch containing a single ledger with
// the given close time — matching the on-disk format that pointAt decodes from the datastore.
func encodedBatch(t *testing.T, seq uint32, closeTime time.Time) []byte {
	t.Helper()
	batch := xdr.LedgerCloseMetaBatch{
		StartSequence: xdr.Uint32(seq),
		EndSequence:   xdr.Uint32(seq),
		LedgerCloseMetas: []xdr.LedgerCloseMeta{
			{
				V: 0,
				V0: &xdr.LedgerCloseMetaV0{
					LedgerHeader: xdr.LedgerHeaderHistoryEntry{
						Header: xdr.LedgerHeader{
							LedgerSeq: xdr.Uint32(seq),
							ScpValue: xdr.StellarValue{
								CloseTime: xdr.TimePoint(closeTime.Unix()),
							},
						},
					},
				},
			},
		},
	}

	var buf bytes.Buffer
	_, err := compressxdr.NewXDREncoder(compressxdr.DefaultCompressor, batch).WriteTo(&buf)
	require.NoError(t, err)
	return buf.Bytes()
}

// expectGetFile sets up a mock expectation that returns a freshly-encoded batch for the given
// sequence. Each call gets its own io.ReadCloser because pointAt closes the reader.
func expectGetFile(t *testing.T, ds *datastore.MockDataStore, seq uint32, closeTime time.Time) *mock.Call {
	t.Helper()
	payload := encodedBatch(t, seq, closeTime)
	key := testSchema.GetObjectKeyFromSequenceNumber(seq)
	return ds.On("GetFile", mock.Anything, key).Return(
		io.NopCloser(bytes.NewReader(payload)), nil,
	)
}

func newFinder(ds datastore.DataStore) *ledgerFinder {
	return &ledgerFinder{
		ds:     ds,
		schema: testSchema,
		cache:  map[uint32]ledgerPoint{},
	}
}

func TestLedgerFinderPointAtReturnsCloseTime(t *testing.T) {
	ds := &datastore.MockDataStore{}
	defer ds.AssertExpectations(t)

	closeTime := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	expectGetFile(t, ds, 100, closeTime).Once()

	pt, err := newFinder(ds).pointAt(context.Background(), 100)
	require.NoError(t, err)
	assert.Equal(t, uint32(100), pt.seq)
	assert.Equal(t, closeTime.Unix(), pt.closeTime.Unix())
}

func TestLedgerFinderPointAtCachesRepeatCalls(t *testing.T) {
	ds := &datastore.MockDataStore{}
	defer ds.AssertExpectations(t)

	closeTime := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	// Once() asserts that a second pointAt for the same seq serves from cache rather than re-fetching.
	expectGetFile(t, ds, 200, closeTime).Once()

	finder := newFinder(ds)
	first, err := finder.pointAt(context.Background(), 200)
	require.NoError(t, err)
	second, err := finder.pointAt(context.Background(), 200)
	require.NoError(t, err)
	assert.Equal(t, first, second)
}

func TestLedgerFinderPointAtGetFileError(t *testing.T) {
	ds := &datastore.MockDataStore{}
	defer ds.AssertExpectations(t)

	key := testSchema.GetObjectKeyFromSequenceNumber(42)
	ds.On("GetFile", mock.Anything, key).Return(nil, errors.New("object not found")).Once()

	_, err := newFinder(ds).pointAt(context.Background(), 42)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unable to fetch ledger 42")
	assert.Contains(t, err.Error(), "object not found")
}

func TestLedgerFinderPointAtDecodeError(t *testing.T) {
	ds := &datastore.MockDataStore{}
	defer ds.AssertExpectations(t)

	key := testSchema.GetObjectKeyFromSequenceNumber(7)
	// Garbage bytes — not a valid zstd stream, so the decoder must fail and the error must surface.
	ds.On("GetFile", mock.Anything, key).Return(
		io.NopCloser(bytes.NewReader([]byte("not a real zstd batch"))), nil,
	).Once()

	_, err := newFinder(ds).pointAt(context.Background(), 7)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unable to decode batch for ledger 7")
}

// findLedgerForTime tests. The boundary predicate returns the first ledger whose close time
// is >= target, matching ledger_range_history_archive.go's findLedgerForTimeBinary so golden
// outputs agree between the two backends.
func TestLedgerFinderFindLedgerForTime(t *testing.T) {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	// Ledger N has close time base + N seconds — monotonically non-decreasing as required.
	closeAt := func(seq uint32) time.Time { return base.Add(time.Duration(seq) * time.Second) }

	tests := []struct {
		name     string
		start    uint32
		end      uint32
		target   time.Time
		expected uint32
	}{
		{
			name:     "target aligns with a ledger close time",
			start:    1,
			end:      16,
			target:   closeAt(10),
			expected: 10,
		},
		{
			name: "sub-second target is floored to ledger with matching Unix second",
			// Close-time comparison is Unix()-based, so the 500ms offset doesn't advance the
			// target past ledger 7. Predicate: prev(6) < 7 && curr(7) >= 7 → returns 7.
			start:    1,
			end:      16,
			target:   closeAt(7).Add(500 * time.Millisecond),
			expected: 7,
		},
		{
			name:     "target at start boundary returns start",
			start:    1,
			end:      16,
			target:   closeAt(1),
			expected: 1,
		},
		{
			name:     "target at end boundary returns end",
			start:    1,
			end:      16,
			target:   closeAt(16),
			expected: 16,
		},
		{
			name:     "start equals end returns start without probing",
			start:    5,
			end:      5,
			target:   closeAt(5),
			expected: 5,
		},
		{
			// Two-element range where target equals start's close time. Under the old recursive
			// search mid == start.seq caused the boundary check to be skipped and the algorithm
			// fell through to pointAt(mid+1), returning end instead of start.
			name:     "two-element range with target at start",
			start:    5,
			end:      6,
			target:   closeAt(5),
			expected: 5,
		},
		{
			name:     "two-element range with target at end",
			start:    5,
			end:      6,
			target:   closeAt(6),
			expected: 6,
		},
		{
			// Target strictly between start and end in a two-element range — the only satisfying
			// seq is end, since end.closeTime is the first >= target.
			name:     "two-element range with target between",
			start:    5,
			end:      6,
			target:   closeAt(5).Add(500 * time.Millisecond).Add(time.Second),
			expected: 6,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := &datastore.MockDataStore{}
			defer ds.AssertExpectations(t)

			// Serve every ledger in the range — we can't predict exactly which seqs the binary
			// search probes, so make them all available and let the cache suppress duplicate GETs.
			for seq := tt.start; seq <= tt.end; seq++ {
				expectGetFile(t, ds, seq, closeAt(seq)).Maybe()
			}

			finder := newFinder(ds)
			startPt := ledgerPoint{seq: tt.start, closeTime: closeAt(tt.start)}
			endPt := ledgerPoint{seq: tt.end, closeTime: closeAt(tt.end)}
			finder.cache[tt.start] = startPt
			finder.cache[tt.end] = endPt

			got, err := finder.findLedgerForTime(context.Background(), tt.target, startPt, endPt)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestLedgerFinderFindLedgerForTimePropagatesDatastoreError(t *testing.T) {
	ds := &datastore.MockDataStore{}
	defer ds.AssertExpectations(t)

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	start := ledgerPoint{seq: 1, closeTime: base.Add(1 * time.Second)}
	end := ledgerPoint{seq: 10, closeTime: base.Add(10 * time.Second)}

	// Any GetFile call (the search will probe at least one interior seq) returns an error.
	ds.On("GetFile", mock.Anything, mock.Anything).Return(nil, errors.New("transient failure"))

	finder := newFinder(ds)
	finder.cache[start.seq] = start
	finder.cache[end.seq] = end

	_, err := finder.findLedgerForTime(context.Background(), base.Add(5*time.Second), start, end)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "transient failure")
}
