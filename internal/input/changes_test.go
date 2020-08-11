package input

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/go/xdr"
)

func TestSendToChannel(t *testing.T) {
	type functionInput struct {
		entry        xdr.LedgerEntry
		accChannel   chan xdr.LedgerEntry
		offChannel   chan xdr.LedgerEntry
		trustChannel chan xdr.LedgerEntry
	}
	type functionOutput struct {
		accEntry   xdr.LedgerEntry
		offEntry   xdr.LedgerEntry
		trustEntry xdr.LedgerEntry
	}

	acc := make(chan xdr.LedgerEntry)
	off := make(chan xdr.LedgerEntry)
	trust := make(chan xdr.LedgerEntry)

	accountTestEntry := xdr.LedgerEntry{
		Data: xdr.LedgerEntryData{
			Type:    xdr.LedgerEntryTypeAccount,
			Account: &xdr.AccountEntry{},
		},
	}
	offerTestEntry := xdr.LedgerEntry{
		Data: xdr.LedgerEntryData{
			Type:  xdr.LedgerEntryTypeOffer,
			Offer: &xdr.OfferEntry{},
		},
	}
	trustTestEntry := xdr.LedgerEntry{
		Data: xdr.LedgerEntryData{
			Type:      xdr.LedgerEntryTypeTrustline,
			TrustLine: &xdr.TrustLineEntry{},
		},
	}

	tests := []struct {
		name string
		args functionInput
		out  functionOutput
	}{
		{
			name: "account",
			args: functionInput{
				entry:        accountTestEntry,
				accChannel:   acc,
				offChannel:   off,
				trustChannel: trust,
			},
			out: functionOutput{
				accEntry: accountTestEntry,
			},
		},
		{
			name: "offer",
			args: functionInput{
				entry:        offerTestEntry,
				accChannel:   acc,
				offChannel:   off,
				trustChannel: trust,
			},
			out: functionOutput{
				offEntry: offerTestEntry,
			},
		},
		{
			name: "trustline",
			args: functionInput{
				entry:        trustTestEntry,
				accChannel:   acc,
				offChannel:   off,
				trustChannel: trust,
			},
			out: functionOutput{
				trustEntry: trustTestEntry,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sendToChannel(tt.args.entry, tt.args.accChannel, tt.args.offChannel, tt.args.trustChannel)
			assert.Equal(t, tt.out.accEntry, <-tt.args.accChannel)
			assert.Equal(t, tt.out.offEntry, <-tt.args.offChannel)
			assert.Equal(t, tt.out.trustEntry, <-tt.args.trustChannel)
		})
	}
}
