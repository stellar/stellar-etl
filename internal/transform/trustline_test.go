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
		{
			wrapTrustlineEntry(xdr.TrustLineEntry{
				Balance:   -1,
				Asset:     nativeTrustLineAsset,
				AccountId: genericAccountID,
			}, 0),
			TrustlineOutput{}, fmt.Errorf("Balance is negative (-1) for trustline (account is %s and asset is native)", genericAccountAddress),
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

func makeTrustlineTestInput() ingest.Change {
	ledgerEntry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 24229503,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeTrustline,
			TrustLine: &xdr.TrustLineEntry{
				AccountId: testAccount1ID,
				Asset:     ethTrustLineAsset,
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
	return ingest.Change{
		Type: xdr.LedgerEntryTypeTrustline,
		Pre:  &xdr.LedgerEntry{},
		Post: &ledgerEntry,
	}
}

func makeTrustlineTestOutput() TrustlineOutput {
	return TrustlineOutput{
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
	}
}

func wrapTrustlineEntry(trustlineEntry xdr.TrustLineEntry, lastModified int) ingest.Change {
	return ingest.Change{
		Type: xdr.LedgerEntryTypeTrustline,
		Pre:  nil,
		Post: &xdr.LedgerEntry{
			LastModifiedLedgerSeq: xdr.Uint32(lastModified),
			Data: xdr.LedgerEntryData{
				Type:      xdr.LedgerEntryTypeTrustline,
				TrustLine: &trustlineEntry,
			},
		},
	}
}
