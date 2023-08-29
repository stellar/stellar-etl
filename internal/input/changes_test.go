package input

import (
	"testing"

	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/log"
	"github.com/stellar/stellar-etl/internal/utils"
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
		entryType: {{Type: entry.Data.Type, Post: &entry}},
	}
	return ChangeBatch{
		Changes: changes,
	}
}

func mockExtractBatch(
	batchStart, batchEnd uint32,
	core *ledgerbackend.CaptiveStellarCore,
	env utils.EnvironmentDetails, logger *utils.EtlLogger) ChangeBatch {
	log.Errorf("mock called")
	return ChangeBatch{
		Changes:    map[xdr.LedgerEntryType][]ingest.Change{},
		BatchStart: batchStart,
		BatchEnd:   batchEnd,
	}
}

func TestStreamChangesBatchNumbers(t *testing.T) {
	type batchRange struct {
		batchStart uint32
		batchEnd   uint32
	}
	type input struct {
		batchStart uint32
		batchEnd   uint32
	}
	type output struct {
		batchRanges []batchRange
	}
	tests := []struct {
		name string
		args input
		out  output
	}{
		{
			name: "single",
			args: input{batchStart: 1, batchEnd: 65},
			out: output{
				batchRanges: []batchRange{
					batchRange{
						batchStart: 1, batchEnd: 65,
					},
				},
			},
		}, {
			name: "one extra",
			args: input{batchStart: 1, batchEnd: 66},
			out: output{
				batchRanges: []batchRange{
					batchRange{
						batchStart: 1, batchEnd: 64,
					}, batchRange{
						batchStart: 65, batchEnd: 66,
					},
				},
			},
		}, {
			name: "multiple",
			args: input{batchStart: 1, batchEnd: 128},
			out: output{
				batchRanges: []batchRange{
					batchRange{
						batchStart: 1, batchEnd: 64,
					},
					batchRange{
						batchStart: 65, batchEnd: 128,
					},
				},
			},
		}, {
			name: "partial",
			args: input{batchStart: 1, batchEnd: 32},
			out: output{
				batchRanges: []batchRange{
					batchRange{
						batchStart: 1, batchEnd: 32,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batchSize := uint32(64)
			changeChan := make(chan ChangeBatch, 10)
			closeChan := make(chan int)
			env := utils.EnvironmentDetails{
				NetworkPassphrase: "",
				ArchiveURLs:       nil,
				BinaryPath:        "",
				CoreConfig:        "",
			}
			logger := utils.NewEtlLogger()
			ExtractBatch = mockExtractBatch
			go StreamChanges(nil, tt.args.batchStart, tt.args.batchEnd, batchSize, changeChan, closeChan, env, logger)
			var got []batchRange
			for b := range changeChan {
				got = append(got, batchRange{
					b.BatchStart,
					b.BatchEnd,
				})
			}
			assert.Equal(t, tt.out.batchRanges, got)
		})
	}
}
