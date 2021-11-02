package input

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

func TestSendBatchToChannel(t *testing.T) {
	type functionInput struct {
		entry          ChangeBatch
		changesChannel chan ChangeBatch
	}
	type functionOutput struct {
		entry *ChangeBatch
	}

	changesChannel := make(chan ChangeBatch)

	accountTestBatch := wrapLedgerEntry(
		xdr.LedgerEntryTypeAccount,
		xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type:    xdr.LedgerEntryTypeAccount,
				Account: &xdr.AccountEntry{},
			},
		})
	offerTestBatch := wrapLedgerEntry(
		xdr.LedgerEntryTypeOffer,
		xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type:  xdr.LedgerEntryTypeOffer,
				Offer: &xdr.OfferEntry{},
			},
		})
	trustTestBatch := wrapLedgerEntry(
		xdr.LedgerEntryTypeTrustline,
		xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type:      xdr.LedgerEntryTypeTrustline,
				TrustLine: &xdr.TrustLineEntry{},
			},
		})
	poolTestBatch := wrapLedgerEntry(
		xdr.LedgerEntryTypeLiquidityPool,
		xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type:          xdr.LedgerEntryTypeLiquidityPool,
				LiquidityPool: &xdr.LiquidityPoolEntry{},
			},
		})

	tests := []struct {
		name string
		args functionInput
		out  functionOutput
	}{
		{
			name: "accounts",
			args: functionInput{
				entry:          accountTestBatch,
				changesChannel: changesChannel,
			},
			out: functionOutput{
				entry: &accountTestBatch,
			},
		},
		{
			name: "offer",
			args: functionInput{
				entry:          offerTestBatch,
				changesChannel: changesChannel,
			},
			out: functionOutput{
				entry: &offerTestBatch,
			},
		},
		{
			name: "trustline",
			args: functionInput{
				entry:          trustTestBatch,
				changesChannel: changesChannel,
			},
			out: functionOutput{
				entry: &trustTestBatch,
			},
		},
		{
			name: "pool",
			args: functionInput{
				entry:          poolTestBatch,
				changesChannel: changesChannel,
			},
			out: functionOutput{
				entry: &poolTestBatch,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			go func() {
				tt.args.changesChannel <- tt.args.entry
			}()
			read := <-tt.args.changesChannel
			assert.Equal(t, *tt.out.entry, read)
		})
	}
}

func wrapLedgerEntry(entryType xdr.LedgerEntryType, entry xdr.LedgerEntry) ChangeBatch {
	changes := map[xdr.LedgerEntryType][]ingest.Change{
		entryType: []ingest.Change{{Type: entry.Data.Type, Post: &entry}},
	}
	return ChangeBatch{
		Changes: changes,
	}
}
