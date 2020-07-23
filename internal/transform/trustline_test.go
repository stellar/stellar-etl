package transform

import (
	"fmt"
	"testing"

	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
)

func TestTransformTrustline(t *testing.T) {
	type transformTest struct {
		input      xdr.LedgerEntry
		wantOutput TrustlineOutput
		wantErr    error
	}

	hardCodedInput, err := prepareHardcodedTrustlineTestInput()
	assert.NoError(t, err)
	hardCodedOutput := prepareHardcodedTrustlineTestOutput()
	tests := []transformTest{
		{
			xdr.LedgerEntry{
				Data: xdr.LedgerEntryData{
					Type: xdr.LedgerEntryTypeOffer,
				},
			},
			TrustlineOutput{}, fmt.Errorf("Could not extract trustline data from ledger entry; actual type is LedgerEntryTypeOffer"),
		},
		{
			wrapTrustlineEntry(xdr.TrustLineEntry{
				Balance:   -1,
				Asset:     hardCodedNativeAsset,
				AccountId: genericAccountID,
			}, 0),
			TrustlineOutput{}, fmt.Errorf("Balance is negative (-1) for trustline"),
		},
		{
			wrapTrustlineEntry(xdr.TrustLineEntry{
				Asset: hardCodedNativeAsset,
			}, 0),
			TrustlineOutput{}, fmt.Errorf("Error running MarshalBinaryCompress when calculating ledger key"),
		},
		{
			hardCodedInput,
			hardCodedOutput, nil,
		},
	}

	for _, test := range tests {
		actualOutput, actualError := TransformTrustline(test.input)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func prepareHardcodedTrustlineTestInput() (ledgerEntry xdr.LedgerEntry, err error) {
	ledgerEntry = xdr.LedgerEntry{
		LastModifiedLedgerSeq: 24229503,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeTrustline,
			TrustLine: &xdr.TrustLineEntry{
				AccountId: hardCodedAccountOneID,
				Asset:     hardCodedETHAsset,
				Balance:   6203000,
				Limit:     9000000000000000000,
				Flags:     1,
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
	return
}

func prepareHardcodedTrustlineTestOutput() TrustlineOutput {
	return TrustlineOutput{
		LedgerKey:          "AAAAAQAAAACI4aa0pXFSj6qfJuIObLw/5zyugLRGYwxb7wFSr3B9eAAAAAFFVEgAAAAAAGfMAIZMO4kWjGqv4Lw0cJ7QIcUFcuL5iGE0IggsIily",
		AccountID:          hardCodedAccountOneAddress,
		AssetType:          1,
		AssetIssuer:        hardCodedAccountThreeAddress,
		AssetCode:          "ETH",
		Balance:            6203000,
		TrustlineLimit:     9000000000000000000,
		Flags:              1,
		BuyingLiabilities:  1000,
		SellingLiabilities: 2000,
		LastModifiedLedger: 24229503,
	}
}

func wrapTrustlineEntry(trustlineEntry xdr.TrustLineEntry, lastModified int) xdr.LedgerEntry {
	return xdr.LedgerEntry{
		LastModifiedLedgerSeq: xdr.Uint32(lastModified),
		Data: xdr.LedgerEntryData{
			Type:      xdr.LedgerEntryTypeTrustline,
			TrustLine: &trustlineEntry,
		},
	}
}
