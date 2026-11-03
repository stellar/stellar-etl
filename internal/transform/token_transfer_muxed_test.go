package transform

import (
	"strings"
	"testing"

	"github.com/stellar/go-stellar-sdk/processors/token_transfer"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TODO: add a token_transfers integration test golden ledger with a SAC transfer
// to a muxed contract (W) destination once Protocol 30 is live on pubnet. These
// tests drive encodeMuxedDestination with hand-built inputs, so nothing here runs
// a real CAP-84 event through TransformTokenTransfer and no golden contains a W.

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
// and Hash variants, and 0 is what the overwhelming majority of muxed rows are
// written as: 3,333,860 of 3,337,675 on 2026-10-25, a single measured day.
// stellar/hubble#726 owns that bug; changing it here would restate those
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

		// A contract destination is the one case where this deliberately
		// diverges from the previous implementation. That code called
		// SetAccountID with a C address, which failed, and wrote an empty
		// string. Taking the contract branch instead yields W(contract, 0).
		//
		// The id is still wrong, in exactly the way #726 describes, but it is
		// no more wrong than the M(account, 0) written for the account case,
		// and it avoids a second code path. Pinned here so the divergence is a
		// recorded decision rather than something a later reader reverts.
		t.Run(test.name+" to a contract destination", func(t *testing.T) {
			address, id, err := encodeMuxedDestination(muxedTestContract, test.info)
			require.NoError(t, err)
			assert.Equal(t, "0", id, "see stellar/hubble#726")

			want := strkey.MuxedContract{}
			require.NoError(t, want.SetContractID(muxedTestContract))
			want.SetID(0)
			wantAddress, err := want.Address()
			require.NoError(t, err)
			assert.Equal(t, wantAddress, address)
			assert.True(t, strings.HasPrefix(address, "W"))
		})
	}
}

// TestEncodeMuxedDestinationUnmultiplexable covers every destination that has
// no multiplexed form. All of them must report no muxed address rather than
// returning an error.
//
// The reason is blast radius, not tidiness: an error here aborts
// transformEvents and drops every token transfer in the ledger, and Airflow
// passes --strict-export unconditionally, so a retry reprocesses the same
// ledger and fails identically. The previous implementation discarded its
// errors and wrote an empty string for all of these, and that tolerance turned
// out to be load bearing.
func TestEncodeMuxedDestinationUnmultiplexable(t *testing.T) {
	muxedAccount := strkey.MuxedAccount{}
	require.NoError(t, muxedAccount.SetAccountID(muxedTestAccount))
	muxedAccount.SetID(1)
	mAddress, err := muxedAccount.Address()
	require.NoError(t, err)

	muxedContract := strkey.MuxedContract{}
	require.NoError(t, muxedContract.SetContractID(muxedTestContract))
	muxedContract.SetID(1)
	wAddress, err := muxedContract.Address()
	require.NoError(t, err)

	for _, test := range []struct {
		name string
		to   string
	}{
		{
			// Burn, clawback and fee events reach here with no destination:
			// the SDK attaches ToMuxedInfo from the data map before it knows
			// the event type, and those events never set "to". A custom SEP-41
			// token emitting a burn with a to_muxed_id key hits this.
			name: "empty destination",
			to:   "",
		},
		{"not an address at all", "not-an-address"},
		{"liquidity pool", strkey.MustEncode(strkey.VersionByteLiquidityPool, make([]byte, 32))},
		{"claimable balance", strkey.MustEncode(strkey.VersionByteClaimableBalance, make([]byte, 33))},
		{"already a muxed account", mAddress},
		{"already a muxed contract", wAddress},
	} {
		t.Run(test.name, func(t *testing.T) {
			address, id, err := encodeMuxedDestination(test.to, muxedInfoID(muxedTestID))
			require.NoError(t, err, "an unmultiplexable destination must not fail the export")
			assert.Equal(t, "", address)
			assert.Equal(t, "7", id, "the multiplexing id is still reported")
		})
	}
}

// TestEncodeMuxedDestinationUnknownVariant is the one case that still errors:
// a MuxedInfo oneof carrying something this code has never seen means the SDK
// gained a variant, which is a pin-level change we control.
func TestEncodeMuxedDestinationUnknownVariant(t *testing.T) {
	_, _, err := encodeMuxedDestination(muxedTestAccount, &token_transfer.MuxedInfo{})
	assert.ErrorContains(t, err, "unknown muxed info variant")
}

// TestEncodeMuxedDestinationNullAccount pins the one production case that the
// fail-loud rewrite would otherwise have turned into an outage.
//
// GAAAA…WHF is the null account: a valid G strkey whose ed25519 key is 32 zero
// bytes. strkey.MuxedAccount.SetAccountID accepts it, then Address() rejects it
// with "muxed account has no ed25519 key". Sending to it is how value is
// destroyed without a burn event, and those transfers carry
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
