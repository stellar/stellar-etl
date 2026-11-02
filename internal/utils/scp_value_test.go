package utils

import (
	"encoding/base64"
	"testing"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
)

// A real pubnet validator address, used so the test exercises the strkey
// encoder rather than comparing the helper against itself.
const testNodeAddress = "GBF7QOLFPTHUEDUPTT4ZTULDTA3QXDIO75JHKJN2IYD7YGQLYUTR75BT"

const (
	// Whole seconds, as carried by StellarValue.closeTime on every arm.
	testCloseTime = 1594584547
	// What an _MS arm carries. Deliberately not a multiple of 1000: a
	// closed_at_ms derived from the seconds field could never produce this
	// value, so asserting on it catches anyone who reaches for
	// closedAt.UnixMilli() instead of the helper.
	testCloseTimeMs = 1594584547123
)

func testSignature() xdr.LedgerCloseValueSignature {
	raw, err := strkey.Decode(strkey.VersionByteAccountID, testNodeAddress)
	if err != nil {
		panic(err)
	}
	var ed xdr.Uint256
	copy(ed[:], raw)

	return xdr.LedgerCloseValueSignature{
		NodeId:    xdr.NodeId{Type: xdr.PublicKeyTypePublicKeyTypeEd25519, Ed25519: &ed},
		Signature: xdr.Signature{0xde, 0xad, 0xbe, 0xef},
	}
}

func headerWithExt(ext xdr.StellarValueExt) xdr.LedgerHeaderHistoryEntry {
	return xdr.LedgerHeaderHistoryEntry{
		Header: xdr.LedgerHeader{
			ScpValue: xdr.StellarValue{CloseTime: testCloseTime, Ext: ext},
		},
	}
}

// TestScpValueInfoFromHeader covers all five arms of the StellarValue.ext
// union. Arms 3 and 4 are the ones CAP-88 added and the ones that silently
// emptied node_id before this helper existed, but arm 2 is already live on
// pubnet and arm 0 covers roughly half of history, so all five are asserted.
func TestScpValueInfoFromHeader(t *testing.T) {
	sig := testSignature()
	wantSignature := base64.StdEncoding.EncodeToString(sig.Signature)

	tests := []struct {
		name            string
		ext             xdr.StellarValueExt
		wantNodeID      string
		wantSignature   string
		wantCloseTimeMs int64
	}{
		{
			// Carries no signature by design. Empty strings are correct here,
			// and erroring instead would break every backfill over old ledgers.
			name:            "arm 0 STELLAR_VALUE_BASIC",
			ext:             xdr.StellarValueExt{V: xdr.StellarValueTypeStellarValueBasic},
			wantNodeID:      "",
			wantSignature:   "",
			wantCloseTimeMs: testCloseTime * 1000,
		},
		{
			name: "arm 1 STELLAR_VALUE_SIGNED",
			ext: xdr.StellarValueExt{
				V:                xdr.StellarValueTypeStellarValueSigned,
				LcValueSignature: &sig,
			},
			wantNodeID:      testNodeAddress,
			wantSignature:   wantSignature,
			wantCloseTimeMs: testCloseTime * 1000,
		},
		{
			// CAP-83, live on pubnet since 2026-09-16 and never read before.
			name: "arm 2 STELLAR_VALUE_EMPTY_TX_SET",
			ext: xdr.StellarValueExt{
				V:             xdr.StellarValueTypeStellarValueEmptyTxSet,
				ProposedValue: &xdr.StellarValueProposedValue{LcValueSignature: sig},
			},
			wantNodeID:      testNodeAddress,
			wantSignature:   wantSignature,
			wantCloseTimeMs: testCloseTime * 1000,
		},
		{
			// The normal case after the Protocol 30 upgrade.
			name: "arm 3 STELLAR_VALUE_SIGNED_MS",
			ext: xdr.StellarValueExt{
				V: xdr.StellarValueTypeStellarValueSignedMs,
				SignedMsValue: &xdr.StellarValueSignedMsValue{
					CloseTimeMs:      xdr.TimePointMs(testCloseTimeMs),
					LcValueSignature: sig,
				},
			},
			wantNodeID:      testNodeAddress,
			wantSignature:   wantSignature,
			wantCloseTimeMs: testCloseTimeMs,
		},
		{
			// Mandatory after the upgrade, not defensive coverage: CAP-88 makes
			// the whole-second arms invalid once the previous ledger is at or
			// above its version.
			name: "arm 4 STELLAR_VALUE_EMPTY_TX_SET_MS",
			ext: xdr.StellarValueExt{
				V: xdr.StellarValueTypeStellarValueEmptyTxSetMs,
				ProposedMsValue: &xdr.StellarValueProposedMsValue{
					CloseTimeMs:      xdr.TimePointMs(testCloseTimeMs),
					LcValueSignature: sig,
				},
			},
			wantNodeID:      testNodeAddress,
			wantSignature:   wantSignature,
			wantCloseTimeMs: testCloseTimeMs,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ScpValueInfoFromHeader(headerWithExt(test.ext))
			assert.NoError(t, err)
			assert.Equal(t, test.wantNodeID, got.NodeID)
			assert.Equal(t, test.wantSignature, got.Signature)
			assert.Equal(t, test.wantCloseTimeMs, got.CloseTimeMs)
		})
	}
}

// TestScpValueInfoSubSecondPrecisionSurvives is the guard against deriving
// closed_at_ms from the seconds field. Any such derivation is a multiple of
// 1000; a real _MS arm is not, and that difference is the only direct evidence
// from BigQuery that the new decode path is live.
func TestScpValueInfoSubSecondPrecisionSurvives(t *testing.T) {
	sig := testSignature()

	for _, ext := range []xdr.StellarValueExt{
		{
			V: xdr.StellarValueTypeStellarValueSignedMs,
			SignedMsValue: &xdr.StellarValueSignedMsValue{
				CloseTimeMs:      xdr.TimePointMs(testCloseTimeMs),
				LcValueSignature: sig,
			},
		},
		{
			V: xdr.StellarValueTypeStellarValueEmptyTxSetMs,
			ProposedMsValue: &xdr.StellarValueProposedMsValue{
				CloseTimeMs:      xdr.TimePointMs(testCloseTimeMs),
				LcValueSignature: sig,
			},
		},
	} {
		got, err := ScpValueInfoFromHeader(headerWithExt(ext))
		assert.NoError(t, err)
		assert.NotEqual(t, int64(0), got.CloseTimeMs%1000,
			"close time in ms must retain sub-second precision on an _MS arm")
		assert.NotEqual(t, int64(testCloseTime)*1000, got.CloseTimeMs,
			"close time in ms must come from the ext arm, not from closeTime * 1000")
	}
}

// TestScpValueInfoFromHeaderErrors asserts the failure modes are loud. The bug
// this helper replaces was silent, so a malformed or unknown arm must return an
// error rather than an empty value.
func TestScpValueInfoFromHeaderErrors(t *testing.T) {
	tests := []struct {
		name    string
		ext     xdr.StellarValueExt
		wantErr string
	}{
		{
			name:    "unknown arm",
			ext:     xdr.StellarValueExt{V: xdr.StellarValueType(99)},
			wantErr: "unknown StellarValue.ext arm: 99",
		},
		{
			name:    "arm 1 with no payload",
			ext:     xdr.StellarValueExt{V: xdr.StellarValueTypeStellarValueSigned},
			wantErr: "STELLAR_VALUE_SIGNED with no lcValueSignature",
		},
		{
			name:    "arm 2 with no payload",
			ext:     xdr.StellarValueExt{V: xdr.StellarValueTypeStellarValueEmptyTxSet},
			wantErr: "STELLAR_VALUE_EMPTY_TX_SET with no proposedValue",
		},
		{
			name:    "arm 3 with no payload",
			ext:     xdr.StellarValueExt{V: xdr.StellarValueTypeStellarValueSignedMs},
			wantErr: "STELLAR_VALUE_SIGNED_MS with no signedMsValue",
		},
		{
			name:    "arm 4 with no payload",
			ext:     xdr.StellarValueExt{V: xdr.StellarValueTypeStellarValueEmptyTxSetMs},
			wantErr: "STELLAR_VALUE_EMPTY_TX_SET_MS with no proposedMsValue",
		},
		{
			// PublicKeyTypeEd25519 is 0, so a zero NodeId selects the Ed25519
			// arm with a nil key and the generated accessor dereferences it.
			name: "signature with a zero node id",
			ext: xdr.StellarValueExt{
				V:             xdr.StellarValueTypeStellarValueSignedMs,
				SignedMsValue: &xdr.StellarValueSignedMsValue{},
			},
			wantErr: "node id is typed Ed25519 but carries no key",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ScpValueInfoFromHeader(headerWithExt(test.ext))
			assert.EqualError(t, err, test.wantErr)
		})
	}
}

func TestScpValueInfoFromLedgerCloseMeta(t *testing.T) {
	sig := testSignature()
	header := headerWithExt(xdr.StellarValueExt{
		V: xdr.StellarValueTypeStellarValueSignedMs,
		SignedMsValue: &xdr.StellarValueSignedMsValue{
			CloseTimeMs:      xdr.TimePointMs(testCloseTimeMs),
			LcValueSignature: sig,
		},
	})

	got, err := ScpValueInfoFromLedgerCloseMeta(xdr.LedgerCloseMeta{
		V:  1,
		V1: &xdr.LedgerCloseMetaV1{LedgerHeader: header},
	})
	assert.NoError(t, err)
	assert.Equal(t, testNodeAddress, got.NodeID)
	assert.Equal(t, int64(testCloseTimeMs), got.CloseTimeMs)
}
