package transform

import (
	"fmt"
	"testing"

	ingestio "github.com/stellar/go/ingest/io"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
)

func TestTransformOffer(t *testing.T) {
	type transformTest struct {
		input      ingestio.Change
		wantOutput OfferOutput
		wantErr    error
	}

	hardCodedInput, err := makeOfferTestInput()
	assert.NoError(t, err)
	hardCodedOutput := makeOfferTestOutput()

	tests := []transformTest{
		{
			ingestio.Change{
				Type: xdr.LedgerEntryTypeAccount,
				Post: &xdr.LedgerEntry{
					Data: xdr.LedgerEntryData{
						Type: xdr.LedgerEntryTypeAccount,
					},
				},
			},
			OfferOutput{}, fmt.Errorf("Could not extract offer data from ledger entry; actual type is LedgerEntryTypeAccount"),
		},
		{
			wrapOfferEntry(xdr.OfferEntry{
				SellerId: genericAccountID,
				OfferId:  -1,
			}, 0),
			OfferOutput{}, fmt.Errorf("OfferID is negative (-1) for offer from account: %s", genericAccountAddress),
		},
		{
			wrapOfferEntry(xdr.OfferEntry{
				SellerId: genericAccountID,
				Amount:   -2,
			}, 0),
			OfferOutput{}, fmt.Errorf("Amount is negative (-2) for offer 0"),
		},
		{
			wrapOfferEntry(xdr.OfferEntry{
				SellerId: genericAccountID,
				Price: xdr.Price{
					N: -3,
					D: 10,
				},
			}, 0),
			OfferOutput{}, fmt.Errorf("Price numerator is negative (-3) for offer 0"),
		},
		{
			wrapOfferEntry(xdr.OfferEntry{
				SellerId: genericAccountID,
				Price: xdr.Price{
					N: 5,
					D: -4,
				},
			}, 0),
			OfferOutput{}, fmt.Errorf("Price denominator is negative (-4) for offer 0"),
		},
		{
			wrapOfferEntry(xdr.OfferEntry{
				SellerId: genericAccountID,
				Price: xdr.Price{
					N: 5,
					D: 0,
				},
			}, 0),
			OfferOutput{}, fmt.Errorf("Price denominator is 0 for offer 0"),
		},
		{
			hardCodedInput,
			hardCodedOutput, nil,
		},
	}

	for _, test := range tests {
		actualOutput, actualError := TransformOffer(test.input)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func wrapOfferEntry(offerEntry xdr.OfferEntry, lastModified int) ingestio.Change {
	return ingestio.Change{
		Type: xdr.LedgerEntryTypeOffer,
		Pre:  nil,
		Post: &xdr.LedgerEntry{
			LastModifiedLedgerSeq: xdr.Uint32(lastModified),
			Data: xdr.LedgerEntryData{
				Type:  xdr.LedgerEntryTypeOffer,
				Offer: &offerEntry,
			},
		},
	}
}

func makeOfferTestInput() (ledgerChange ingestio.Change, err error) {
	ledgerChange = ingestio.Change{
		Type: xdr.LedgerEntryTypeOffer,
		Pre: &xdr.LedgerEntry{
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
		Post: nil,
	}
	return
}

func makeOfferTestOutput() OfferOutput {
	return OfferOutput{
		SellerID:           testAccount1Address,
		OfferID:            260678439,
		SellingAsset:       "AAAAAA==",
		BuyingAsset:        "AAAAAUVUSAAAAAAAZ8wAhkw7iRaMaq/gvDRwntAhxQVy4vmIYTQiCCwiKXI=",
		Amount:             2628450327,
		PriceN:             920936891,
		PriceD:             1790879058,
		Price:              0.5142373444404865,
		Flags:              2,
		LastModifiedLedger: 30715263,
		Deleted:            true,
	}
}
