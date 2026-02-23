package transform

import (
	"fmt"
	"testing"
	"time"

	"github.com/guregu/null"
	"github.com/stretchr/testify/assert"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestTransformOffer(t *testing.T) {
	type inputStruct struct {
		ingest ingest.Change
	}
	type transformTest struct {
		input      inputStruct
		wantOutput OfferOutput
		wantErr    error
	}

	hardCodedInput, err := makeOfferTestInput()
	assert.NoError(t, err)
	hardCodedOutput := makeOfferTestOutput()

	tests := []transformTest{
		{
			inputStruct{ingest.Change{
				ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
				Type:       xdr.LedgerEntryTypeAccount,
				Post: &xdr.LedgerEntry{
					Data: xdr.LedgerEntryData{
						Type: xdr.LedgerEntryTypeAccount,
					},
				},
			},
			},
			OfferOutput{}, fmt.Errorf("could not extract offer data from ledger entry; actual type is LedgerEntryTypeAccount"),
		},
		{
			inputStruct{wrapOfferEntry(xdr.OfferEntry{
				SellerId: genericAccountID,
				OfferId:  -1,
			}, 0),
			},
			OfferOutput{}, fmt.Errorf("offerID is negative (-1) for offer from account: %s", genericAccountAddress),
		},
		{
			inputStruct{wrapOfferEntry(xdr.OfferEntry{
				SellerId: genericAccountID,
				Amount:   -2,
			}, 0),
			},
			OfferOutput{}, fmt.Errorf("amount is negative (-2) for offer 0"),
		},
		{
			inputStruct{wrapOfferEntry(xdr.OfferEntry{
				SellerId: genericAccountID,
				Price: xdr.Price{
					N: -3,
					D: 10,
				},
			}, 0),
			},
			OfferOutput{}, fmt.Errorf("price numerator is negative (-3) for offer 0"),
		},
		{
			inputStruct{wrapOfferEntry(xdr.OfferEntry{
				SellerId: genericAccountID,
				Price: xdr.Price{
					N: 5,
					D: -4,
				},
			}, 0),
			},
			OfferOutput{}, fmt.Errorf("price denominator is negative (-4) for offer 0"),
		},
		{
			inputStruct{wrapOfferEntry(xdr.OfferEntry{
				SellerId: genericAccountID,
				Price: xdr.Price{
					N: 5,
					D: 0,
				},
			}, 0),
			},
			OfferOutput{}, fmt.Errorf("price denominator is 0 for offer 0"),
		},
		{
			inputStruct{
				hardCodedInput,
			},
			hardCodedOutput, nil,
		},
	}

	for _, test := range tests {
		header := xdr.LedgerHeaderHistoryEntry{
			Header: xdr.LedgerHeader{
				ScpValue: xdr.StellarValue{
					CloseTime: 1000,
				},
				LedgerSeq: 10,
			},
		}
		actualOutput, actualError := TransformOffer(test.input.ingest, header)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func wrapOfferEntry(offerEntry xdr.OfferEntry, lastModified int) ingest.Change {
	return ingest.Change{
		ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
		Type:       xdr.LedgerEntryTypeOffer,
		Pre:        nil,
		Post: &xdr.LedgerEntry{
			LastModifiedLedgerSeq: xdr.Uint32(lastModified),
			Data: xdr.LedgerEntryData{
				Type:  xdr.LedgerEntryTypeOffer,
				Offer: &offerEntry,
			},
		},
	}
}

func makeOfferTestInput() (ledgerChange ingest.Change, err error) {
	ledgerChange = ingest.Change{
		ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryRemoved,
		Type:       xdr.LedgerEntryTypeOffer,
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
			Ext: xdr.LedgerEntryExt{
				V: 1,
				V1: &xdr.LedgerEntryExtensionV1{
					SponsoringId: &testAccount3ID,
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
		SellingAssetType:   "native",
		SellingAssetCode:   "",
		SellingAssetIssuer: "",
		SellingAssetID:     -5706705804583548011,
		BuyingAssetType:    "credit_alphanum4",
		BuyingAssetCode:    "ETH",
		BuyingAssetIssuer:  testAccount3Address,
		BuyingAssetID:      4476940172956910889,
		Amount:             262.8450327,
		PriceN:             920936891,
		PriceD:             1790879058,
		Price:              0.5142373444404865,
		Flags:              2,
		LastModifiedLedger: 30715263,
		LedgerEntryChange:  2,
		Deleted:            true,
		Sponsor:            null.StringFrom(testAccount3Address),
		LedgerSequence:     10,
		ClosedAt:           time.Date(1970, time.January, 1, 0, 16, 40, 0, time.UTC),
	}
}
