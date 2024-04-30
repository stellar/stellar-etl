package transform

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

func TestTransformTrustline(t *testing.T) {
	type inputStruct struct {
		ingest ingest.Change
	}
	type transformTest struct {
		input      inputStruct
		wantOutput TrustlineOutput
		wantErr    error
	}

	hardCodedInput := makeTrustlineTestInput()
	hardCodedOutput := makeTrustlineTestOutput()

	tests := []transformTest{
		{
			inputStruct{
				ingest.Change{
					Type: xdr.LedgerEntryTypeOffer,
					Pre:  nil,
					Post: &xdr.LedgerEntry{
						Data: xdr.LedgerEntryData{
							Type: xdr.LedgerEntryTypeOffer,
						},
					},
				},
			},
			TrustlineOutput{}, fmt.Errorf("could not extract trustline data from ledger entry; actual type is LedgerEntryTypeOffer"),
		},
	}

	for i := range hardCodedInput {
		tests = append(tests, transformTest{
			input:      inputStruct{hardCodedInput[i]},
			wantOutput: hardCodedOutput[i],
			wantErr:    nil,
		})
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
		actualOutput, actualError := TransformTrustline(test.input.ingest, header)
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
			AssetID:            -2311386320395871674,
			Balance:            0.6203,
			TrustlineLimit:     9000000000000000000,
			Flags:              1,
			BuyingLiabilities:  0.0001,
			SellingLiabilities: 0.0002,
			LastModifiedLedger: 24229503,
			LedgerEntryChange:  1,
			Deleted:            false,
			LedgerSequence:     10,
			ClosedAt:           time.Date(1970, time.January, 1, 0, 16, 40, 0, time.UTC),
		},
		{
			LedgerKey:          "AAAAAQAAAAAcR0GXGO76pFs4y38vJVAanjnLg4emNun7zAx0pHcDGAAAAAMBAwQFBwkAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==",
			AccountID:          testAccount2Address,
			AssetType:          3,
			AssetID:            -1967220342708457407,
			Balance:            0.5,
			TrustlineLimit:     1111111111111111111,
			LiquidityPoolID:    "0103040507090000000000000000000000000000000000000000000000000000",
			Flags:              1,
			BuyingLiabilities:  0.0015,
			SellingLiabilities: 0.0005,
			LastModifiedLedger: 123456789,
			LedgerEntryChange:  1,
			Deleted:            false,
			LedgerSequence:     10,
			ClosedAt:           time.Date(1970, time.January, 1, 0, 16, 40, 0, time.UTC),
		},
	}
}
