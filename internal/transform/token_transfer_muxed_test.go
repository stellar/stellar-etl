package transform

import (
	"strings"
	"testing"

	"github.com/stellar/go-stellar-sdk/processors/token_transfer"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A real pubnet account and a valid contract address, so the tests exercise the
// strkey decoders rather than agreeing with whatever the encoder produced.
const (
	muxedTestAccount  = "GBF7QOLFPTHUEDUPTT4ZTULDTA3QXDIO75JHKJN2IYD7YGQLYUTR75BT"
	muxedTestContract = "CAAACAQDAQCQMBYIBEFAWDANBYHRAEISCMKBKFQXDAMRUGY4DUPB6N4O"
	muxedTestID       = uint64(7)
)

func muxedInfoID(id uint64) *token_transfer.MuxedInfo {
	return &token_transfer.MuxedInfo{Content: &token_transfer.MuxedInfo_Id{Id: id}}
}

// TestEncodeMuxedDestinationContract is the CAP-84 case. A transfer to a muxed
// contract reports the plain C address in the event topics and the id in the
// data map, so the W strkey only exists if we rebuild it. Before this branch
// existed, strkey.MuxedAccount.SetAccountID rejected the C address and both
// errors were discarded, leaving to_muxed as an empty string.
func TestEncodeMuxedDestinationContract(t *testing.T) {
	address, id, err := encodeMuxedDestination(muxedTestContract, muxedInfoID(muxedTestID))
	require.NoError(t, err)

	assert.True(t, strings.HasPrefix(address, "W"), "a muxed contract must encode to a W strkey, got %q", address)
	assert.Equal(t, "7", id)

	// Round-trip: the W must decode back to exactly the contract and id we fed in.
	decoded, err := strkey.DecodeMuxedContract(address)
	require.NoError(t, err)
	contractID, err := decoded.ContractID()
	require.NoError(t, err)
	assert.Equal(t, muxedTestContract, contractID)
	assert.Equal(t, muxedTestID, decoded.ID())
}

// TestEncodeMuxedDestinationAccount asserts the CAP-67 path is untouched. This
// is the only path with production volume today, so any change here would
// restate millions of rows a day.
func TestEncodeMuxedDestinationAccount(t *testing.T) {
	address, id, err := encodeMuxedDestination(muxedTestAccount, muxedInfoID(muxedTestID))
	require.NoError(t, err)

	assert.True(t, strings.HasPrefix(address, "M"), "an account must still encode to an M strkey, got %q", address)
	assert.Equal(t, "7", id)

	decoded, err := strkey.DecodeMuxedAccount(address)
	require.NoError(t, err)
	accountID, err := decoded.AccountID()
	require.NoError(t, err)
	assert.Equal(t, muxedTestAccount, accountID)
	assert.Equal(t, muxedTestID, decoded.ID())
}

// TestEncodeMuxedDestinationMemoVariantsUnchanged pins the behaviour this
// release deliberately does not fix. MuxedInfo.GetId() reports 0 for the Text
// and Hash variants, and 0 is what roughly 3.33 million rows a day are written
// as. stellar/hubble#726 owns that bug; changing it here would restate those
// rows as a side effect of a dated protocol release.
func TestEncodeMuxedDestinationMemoVariantsUnchanged(t *testing.T) {
	for _, test := range []struct {
		name string
		info *token_transfer.MuxedInfo
	}{
		{"text memo", &token_transfer.MuxedInfo{Content: &token_transfer.MuxedInfo_Text{Text: "invoice-42"}}},
		{"hash memo", &token_transfer.MuxedInfo{Content: &token_transfer.MuxedInfo_Hash{Hash: []byte{0x01, 0x02}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			address, id, err := encodeMuxedDestination(muxedTestAccount, test.info)
			require.NoError(t, err)

			assert.Equal(t, "0", id, "see stellar/hubble#726")

			// Byte-identical to what the previous implementation produced:
			// an M strkey built from the destination with a zero id.
			want := strkey.MuxedAccount{}
			require.NoError(t, want.SetAccountID(muxedTestAccount))
			want.SetID(0)
			wantAddress, err := want.Address()
			require.NoError(t, err)
			assert.Equal(t, wantAddress, address)
		})
	}
}

// TestEncodeMuxedDestinationErrors asserts the failure modes are loud. The bug
// this replaces discarded two errors on consecutive lines and wrote an empty
// string, which is indistinguishable downstream from having no muxed info at
// all once int_token_transfer_enrichment coalesces it.
func TestEncodeMuxedDestinationErrors(t *testing.T) {
	t.Run("undecodable destination", func(t *testing.T) {
		_, _, err := encodeMuxedDestination("not-an-address", muxedInfoID(muxedTestID))
		assert.ErrorContains(t, err, "could not encode muxed destination")
	})

	t.Run("empty destination", func(t *testing.T) {
		_, _, err := encodeMuxedDestination("", muxedInfoID(muxedTestID))
		assert.ErrorContains(t, err, "could not encode muxed destination")
	})

	t.Run("unset variant", func(t *testing.T) {
		_, _, err := encodeMuxedDestination(muxedTestAccount, &token_transfer.MuxedInfo{})
		assert.ErrorContains(t, err, "unknown muxed info variant")
	})
}

// TestEncodeMuxedDestinationNullAccount pins the one production case that the
// fail-loud rewrite would otherwise have turned into an outage.
//
// GAAAA…WHF is the null account: a valid G strkey whose ed25519 key is 32 zero
// bytes. strkey.MuxedAccount.SetAccountID accepts it, then Address() rejects it
// with "muxed account has no ed25519 key". Transfers to it are burns that carry
// a memo, so they populate ToMuxedInfo and reach this function. Returning that
// error would abort transformEvents and drop every token transfer in the
// ledger, and with --strict-export always set by Airflow that is a fatal export
// that retries cannot clear.
func TestEncodeMuxedDestinationNullAccount(t *testing.T) {
	const nullAccount = "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"

	address, id, err := encodeMuxedDestination(nullAccount, muxedInfoID(muxedTestID))
	require.NoError(t, err, "a transfer to the null account must not fail the export")

	// Byte-identical to what the previous implementation wrote for these rows.
	assert.Equal(t, "", address)
	assert.Equal(t, "7", id)
}
