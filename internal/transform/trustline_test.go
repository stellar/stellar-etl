package transform

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

func TestTransformTrustline(t *testing.T) {
	type transformTest struct {
		input      ingest.Change
		wantOutput TrustlineOutput
		wantErr    error
	}

	hardCodedInput := makeTrustlineTestInput()
	hardCodedOutput := makeTrustlineTestOutput()
	tests := []transformTest{
		{
			ingest.Change{
				Type: xdr.LedgerEntryTypeOffer,
				Pre:  nil,
				Post: &xdr.LedgerEntry{
					Data: xdr.LedgerEntryData{
						Type: xdr.LedgerEntryTypeOffer,
					},
				},
			},
			TrustlineOutput{}, fmt.Errorf("Could not extract trustline data from ledger entry; actual type is LedgerEntryTypeOffer"),
		},
	}

	for i := range hardCodedInput {
		tests = append(tests, transformTest{
			input:      hardCodedInput[i],
			wantOutput: hardCodedOutput[i],
			wantErr:    nil,
		})
	}

	for _, test := range tests {
		actualOutput, actualError := TransformTrustline(test.input)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func makeTrustlineTestInput() []ingest.Change {
	assetLedgerEntry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 24229503,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeTrustline,
			TrustLine: &xdr.TrustLineEntry{
				AccountId: testAccount1ID,
				Asset:     ethTrustLineAsset,
				Balance:   6203000,
				Limit:     9000000000000000000,
				Flags:     xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag),
				Ext: xdr.TrustLineEntryExt{
					V: 1,
					V1: &xdr.TrustLineEntryV1{
						Liabilities: xdr.Liabilities{
							Buying:  1000,
							Selling: 2000,
						},
					},
				},
			},
		},
	}
	lpLedgerEntry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 123456789,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeTrustline,
			TrustLine: &xdr.TrustLineEntry{
				AccountId: testAccount2ID,
				Asset:     liquidityPoolAsset,
				Balance:   5000000,
				Limit:     1111111111111111111,
				Flags:     xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag),
				Ext: xdr.TrustLineEntryExt{
					V: 1,
					V1: &xdr.TrustLineEntryV1{
						Liabilities: xdr.Liabilities{
							Buying:  15000,
							Selling: 5000,
						},
					},
				},
			},
		},
	}
	return []ingest.Change{
		{
			Type: xdr.LedgerEntryTypeTrustline,
			Pre:  &xdr.LedgerEntry{},
			Post: &assetLedgerEntry,
		},
		{
			Type: xdr.LedgerEntryTypeTrustline,
			Pre:  &xdr.LedgerEntry{},
			Post: &lpLedgerEntry,
		},
	}
}

func makeTrustlineTestOutput() []TrustlineOutput {
	return []TrustlineOutput{
		{
			LedgerKey:          "AAAAAQAAAACI4aa0pXFSj6qfJuIObLw/5zyugLRGYwxb7wFSr3B9eAAAAAFFVEgAAAAAAGfMAIZMO4kWjGqv4Lw0cJ7QIcUFcuL5iGE0IggsIily",
			AccountID:          testAccount1Address,
			AssetType:          1,
			AssetIssuer:        testAccount3Address,
			AssetCode:          "ETH",
			Balance:            6203000,
			TrustlineLimit:     9000000000000000000,
			Flags:              1,
			BuyingLiabilities:  1000,
			SellingLiabilities: 2000,
			LastModifiedLedger: 24229503,
			Deleted:            false,
		},
		{
			LedgerKey:          "AAAAAQAAAAAcR0GXGO76pFs4y38vJVAanjnLg4emNun7zAx0pHcDGAAAAAMBAwQFBwkAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==",
			AccountID:          testAccount2Address,
			AssetType:          3,
			Balance:            5000000,
			TrustlineLimit:     1111111111111111111,
			LiquidityPoolID:    "0103040507090000000000000000000000000000000000000000000000000000",
			Flags:              1,
			BuyingLiabilities:  15000,
			SellingLiabilities: 5000,
			LastModifiedLedger: 123456789,
			Deleted:            false,
		},
	}
}
