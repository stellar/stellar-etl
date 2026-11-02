package transform

import (
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
)

// Every other closed_at_ms assertion in this package runs against a
// STELLAR_VALUE_BASIC fixture, where closeTime*1000 and closedAt.UnixMilli()
// are the same number by definition. Those tests therefore cannot tell a
// correct transform from one that took the shortcut.
//
// The tests below use a STELLAR_VALUE_SIGNED_MS header whose millisecond value
// is deliberately not a multiple of 1000. Only a transform that reads the ext
// arm through the shared helper can produce it; anything derived from the
// whole-second closeTime is always a multiple of 1000 and fails here.
const (
	msArmCloseTime   = 1594584547    // whole seconds, as carried by closeTime
	msArmCloseTimeMs = 1594584547123 // what the _MS arm carries
)

func msArmSignature() xdr.LedgerCloseValueSignature {
	var ed xdr.Uint256
	copy(ed[:], make([]byte, 32))
	return xdr.LedgerCloseValueSignature{
		NodeId:    xdr.NodeId{Type: xdr.PublicKeyTypePublicKeyTypeEd25519, Ed25519: &ed},
		Signature: xdr.Signature{0xde, 0xad, 0xbe, 0xef},
	}
}

func msArmHeader() xdr.LedgerHeaderHistoryEntry {
	return xdr.LedgerHeaderHistoryEntry{
		Header: xdr.LedgerHeader{
			LedgerSeq: 10,
			ScpValue: xdr.StellarValue{
				CloseTime: msArmCloseTime,
				Ext: xdr.StellarValueExt{
					V: xdr.StellarValueTypeStellarValueSignedMs,
					SignedMsValue: &xdr.StellarValueSignedMsValue{
						CloseTimeMs:      xdr.TimePointMs(msArmCloseTimeMs),
						LcValueSignature: msArmSignature(),
					},
				},
			},
		},
	}
}

// TestTtlClosedAtMsUsesScpValueInfo covers the eleven state-table transforms,
// which all receive xdr.LedgerHeaderHistoryEntry and were changed the same way.
func TestTtlClosedAtMsUsesScpValueInfo(t *testing.T) {
	changes := makeTtlTestInput()
	assert.NotEmpty(t, changes)

	output, err := TransformTtl(changes[0], msArmHeader())
	assert.NoError(t, err)

	assert.Equal(t, int64(msArmCloseTimeMs), output.ClosedAtMs)
	assert.NotEqual(t, int64(0), output.ClosedAtMs%1000,
		"closed_at_ms must keep sub-second precision, so it cannot come from closeTime")
	assert.NotEqual(t, output.ClosedAt.UnixMilli(), output.ClosedAtMs,
		"closed_at_ms must not be derived from the closed_at timestamp")

	// closed_at itself is unchanged: CAP-88 left the whole-second field alone.
	assert.Equal(t, int64(msArmCloseTime), output.ClosedAt.Unix())
}

// TestTradeClosedAtMsUsesScpValueInfo covers TransformTrade, the one transform
// that could not previously reach the ext arms: it took an already-collapsed
// time.Time, so sub-second precision was gone before it was called. It now
// takes the header, which is what the TradeTransformInput change is for.
func TestTradeClosedAtMsUsesScpValueInfo(t *testing.T) {
	trades, err := TransformTrade(0, 100, makeTradeTestInput(), msArmHeader())
	assert.NoError(t, err)
	assert.NotEmpty(t, trades, "fixture should produce at least one trade")

	for _, trade := range trades {
		assert.Equal(t, int64(msArmCloseTimeMs), trade.ClosedAtMs)
		assert.NotEqual(t, int64(0), trade.ClosedAtMs%1000,
			"closed_at_ms must keep sub-second precision")
		assert.NotEqual(t, trade.ClosedAt.UnixMilli(), trade.ClosedAtMs,
			"closed_at_ms must not be derived from the closed_at timestamp")

		// Both existing timestamp columns stay whole seconds.
		assert.Equal(t, int64(msArmCloseTime), trade.ClosedAt.Unix())
		assert.Equal(t, int64(msArmCloseTime), trade.LedgerClosedAt.Unix())
	}
}
