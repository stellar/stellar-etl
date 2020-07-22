package transform

import (
	"fmt"
	"testing"

	ingestio "github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
)

func TestTransformOffer(t *testing.T) {
	type transformTest struct {
		input      ingestio.Change
		wantOutput OfferOutput
		wantErr    error
	}
	genericAccountID, err := xdr.NewAccountId(xdr.PublicKeyTypePublicKeyTypeEd25519, xdr.Uint256([32]byte{}))
	assert.NoError(t, err)
	genericAccountAddress, err := genericAccountID.GetAddress()
	assert.NoError(t, err)

	hardCodedInput, err := prepareHardcodedOfferTestInput()
	assert.NoError(t, err)
	hardCodedOutput := prepareHardcodedOfferTestOutput()

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

func prepareHardcodedOfferTestInput() (ledgerChange ingestio.Change, err error) {
	hardCodedAccountID, err := xdr.NewAccountId(xdr.PublicKeyTypePublicKeyTypeEd25519, xdr.Uint256([32]byte{9, 13, 119, 101, 93, 2, 49, 146, 108, 232, 96, 108, 62, 49, 254, 143, 121, 165, 44, 237, 34, 197, 125, 11, 184, 24, 88, 236, 241, 192, 185, 158}))
	if err != nil {
		return
	}

	hardCodedAssetIssuer, err := xdr.NewAccountId(xdr.PublicKeyTypePublicKeyTypeEd25519, xdr.Uint256([32]byte{234, 172, 104, 212, 208, 227, 123, 76, 36, 194, 83, 105, 22, 232, 48, 115, 95, 3, 45, 13, 107, 42, 28, 143, 202, 59, 197, 162, 94, 8, 62, 58}))
	if err != nil {
		return
	}

	hardCodedBuyingAsset := xdr.Asset{
		Type: xdr.AssetTypeAssetTypeCreditAlphanum4,
		AlphaNum4: &xdr.AssetAlphaNum4{
			AssetCode: xdr.AssetCode4([4]byte{66, 82, 76, 0}),
			Issuer:    hardCodedAssetIssuer,
		},
	}

	ledgerChange = ingestio.Change{
		Type: xdr.LedgerEntryTypeOffer,
		Pre:  nil,
		Post: &xdr.LedgerEntry{
			LastModifiedLedgerSeq: xdr.Uint32(30715263),
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeOffer,
				Offer: &xdr.OfferEntry{
					SellerId: hardCodedAccountID,
					OfferId:  260678439,
					Selling: xdr.Asset{
						Type: xdr.AssetTypeAssetTypeNative,
					},
					Buying: hardCodedBuyingAsset,
					Amount: 2628450327,
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

func prepareHardcodedOfferTestOutput() OfferOutput {
	return OfferOutput{
		SellerID:           "GAEQ253FLUBDDETM5BQGYPRR72HXTJJM5URMK7ILXAMFR3HRYC4Z43IR",
		OfferID:            260678439,
		SellingAsset:       "AAAAAA==",
		BuyingAsset:        "AAAAAUJSTAAAAAAA6qxo1NDje0wkwlNpFugwc18DLQ1rKhyPyjvFol4IPjo=",
		Amount:             2628450327,
		PriceN:             920936891,
		PriceD:             1790879058,
		Price:              0.5142373444404865,
		Flags:              2,
		LastModifiedLedger: 30715263,
	}
}
