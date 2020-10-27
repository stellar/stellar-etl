package transform

import (
	"testing"

	ingestio "github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
)

func TestTransformOfferNormalized(t *testing.T) {
	type testInput struct {
		change ingestio.Change
		ledger uint32
	}
	type transformTest struct {
		input      testInput
		wantOutput NormalizedOfferOutput
		wantErr    error
	}

	hardCodedInput, err := makeOfferNormalizedTestInput()
	assert.NoError(t, err)
	hardCodedOutput := makeOfferNormalizedTestOutput()

	tests := []transformTest{
		{
			input: testInput{ingestio.Change{
				Type: xdr.LedgerEntryTypeOffer,
				Pre: &xdr.LedgerEntry{
					LastModifiedLedgerSeq: xdr.Uint32(100),
					Data: xdr.LedgerEntryData{
						Type: xdr.LedgerEntryTypeOffer,
						Offer: &xdr.OfferEntry{
							SellerId: genericAccountID,
							Price: xdr.Price{
								N: 5,
								D: 34,
							},
						},
					},
				},
				Post: nil,
			}, 100},
			wantOutput: NormalizedOfferOutput{},
			wantErr:    nil,
		},
		{
			input:      testInput{hardCodedInput, 100},
			wantOutput: hardCodedOutput,
			wantErr:    nil,
		},
	}

	for _, test := range tests {
		actualOutput, actualError := TransformOfferNormalized(test.input.change, test.input.ledger)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func makeOfferNormalizedTestInput() (ledgerChange ingestio.Change, err error) {
	ledgerChange = ingestio.Change{
		Type: xdr.LedgerEntryTypeOffer,
		Pre:  nil,
		Post: &xdr.LedgerEntry{
			LastModifiedLedgerSeq: xdr.Uint32(30715263),
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeOffer,
				Offer: &xdr.OfferEntry{
					SellerId: testAccount1ID,
					OfferId:  260678439,
					Selling:  nativeAsset,
					Buying:   ethAsset,
					Amount:   2628450327,
					Price: xdr.Price{
						N: 920936891,
						D: 1790879058,
					},
					Flags: 2,
				},
			},
		},
	}
	return
}

func makeOfferNormalizedTestOutput() NormalizedOfferOutput {
	var dimOfferID, marketID, accountID uint64
	dimOfferID = 2427794722528467255
	marketID = 10357275879248593505
	accountID = 4268167189990212240
	return NormalizedOfferOutput{
		Market: DimMarket{
			ID:            marketID,
			BaseCode:      "ETH",
			BaseIssuer:    testAccount3Address,
			CounterCode:   "native",
			CounterIssuer: "",
		},
		Offer: DimOffer{
			HorizonID:     260678439,
			DimOfferID:    dimOfferID,
			MarketID:      marketID,
			MakerID:       accountID,
			Action:        "b",
			BaseAmount:    2628450327,
			CounterAmount: 1351647316.1502085,
			Price:         0.5142373444404865,
		},
		Account: DimAccount{
			Address: testAccount1Address,
			ID:      accountID,
		},
		Event: FactOfferEvent{
			LedgerSeq:       100,
			OfferInstanceID: dimOfferID,
		},
	}
}
