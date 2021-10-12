package input

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

func TestSendBatchToChannel(t *testing.T) {
	type functionInput struct {
		entry        ChangeBatch
		accChannel   chan ChangeBatch
		offChannel   chan ChangeBatch
		trustChannel chan ChangeBatch
		poolChannel  chan ChangeBatch
	}
	type functionOutput struct {
		accEntry   *ChangeBatch
		offEntry   *ChangeBatch
		trustEntry *ChangeBatch
		poolEntry  *ChangeBatch
	}

	acc := make(chan ChangeBatch)
	off := make(chan ChangeBatch)
	trust := make(chan ChangeBatch)
	pool := make(chan ChangeBatch)

	accountTestBatch := wrapLedgerEntry(xdr.LedgerEntry{
		Data: xdr.LedgerEntryData{
			Type:    xdr.LedgerEntryTypeAccount,
			Account: &xdr.AccountEntry{},
		},
	})
	offerTestBatch := wrapLedgerEntry(xdr.LedgerEntry{
		Data: xdr.LedgerEntryData{
			Type:  xdr.LedgerEntryTypeOffer,
			Offer: &xdr.OfferEntry{},
		},
	})
	trustTestBatch := wrapLedgerEntry(xdr.LedgerEntry{
		Data: xdr.LedgerEntryData{
			Type:      xdr.LedgerEntryTypeTrustline,
			TrustLine: &xdr.TrustLineEntry{},
		},
	})
	poolTestBatch := wrapLedgerEntry(xdr.LedgerEntry{
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
			name: "account",
			args: functionInput{
				entry:        accountTestBatch,
				accChannel:   acc,
				offChannel:   off,
				trustChannel: trust,
				poolChannel:  pool,
			},
			out: functionOutput{
				accEntry: &accountTestBatch,
			},
		},
		{
			name: "offer",
			args: functionInput{
				entry:        offerTestBatch,
				accChannel:   acc,
				offChannel:   off,
				trustChannel: trust,
				poolChannel:  pool,
			},
			out: functionOutput{
				offEntry: &offerTestBatch,
			},
		},
		{
			name: "trustline",
			args: functionInput{
				entry:        trustTestBatch,
				accChannel:   acc,
				offChannel:   off,
				trustChannel: trust,
				poolChannel:  pool,
			},
			out: functionOutput{
				trustEntry: &trustTestBatch,
			},
		},
		{
			name: "pool",
			args: functionInput{
				entry:        poolTestBatch,
				accChannel:   acc,
				offChannel:   off,
				trustChannel: trust,
				poolChannel:  pool,
			},
			out: functionOutput{
				poolEntry: &poolTestBatch,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			go sendBatchToChannels(tt.args.entry, tt.args.accChannel, tt.args.offChannel, tt.args.trustChannel, tt.args.poolChannel)

			needToReadAcc := tt.out.accEntry != nil
			needToReadOff := tt.out.offEntry != nil
			needToReadTrust := tt.out.trustEntry != nil
			needToReadPool := tt.out.poolEntry != nil

			for needToReadAcc || needToReadOff || needToReadTrust || needToReadPool {
				select {
				case read := <-tt.args.accChannel:
					assert.Equal(t, *tt.out.accEntry, read)
					needToReadAcc = false
				case read := <-tt.args.offChannel:
					assert.Equal(t, *tt.out.offEntry, read)
					needToReadOff = false
				case read := <-tt.args.trustChannel:
					assert.Equal(t, *tt.out.trustEntry, read)
					needToReadTrust = false
				case read := <-tt.args.poolChannel:
					assert.Equal(t, *tt.out.poolEntry, read)
					needToReadPool = false
				}
			}

		})
	}
}

func wrapLedgerEntry(entry xdr.LedgerEntry) ChangeBatch {
	changes := []ingest.Change{
		{Type: entry.Data.Type, Post: &entry},
	}
	return ChangeBatch{
		Changes: changes,
		Type:    entry.Data.Type,
	}
}
