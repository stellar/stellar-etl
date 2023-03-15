package transform

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

func TestTransformPool(t *testing.T) {
	type transformTest struct {
		input      ingest.Change
		wantOutput PoolOutput
		wantErr    error
	}

	hardCodedInput := makePoolTestInput()
	hardCodedOutput := makePoolTestOutput()

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
			PoolOutput{}, nil,
		},
		{
			hardCodedInput,
			hardCodedOutput, nil,
		},
	}

	for _, test := range tests {
		actualOutput, actualError := TransformPool(test.input)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func wrapPoolEntry(poolEntry xdr.LiquidityPoolEntry, lastModified int) ingest.Change {
	return ingest.Change{
		Type: xdr.LedgerEntryTypeLiquidityPool,
		Pre: &xdr.LedgerEntry{
			LastModifiedLedgerSeq: xdr.Uint32(lastModified),
			Data: xdr.LedgerEntryData{
				Type:          xdr.LedgerEntryTypeLiquidityPool,
				LiquidityPool: &poolEntry,
			},
		},
	}
}

func makePoolTestInput() ingest.Change {
	ledgerEntry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 30705278,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeLiquidityPool,
			LiquidityPool: &xdr.LiquidityPoolEntry{
				LiquidityPoolId: xdr.PoolId{23, 45, 67},
				Body: xdr.LiquidityPoolEntryBody{
					Type: xdr.LiquidityPoolTypeLiquidityPoolConstantProduct,
					ConstantProduct: &xdr.LiquidityPoolEntryConstantProduct{
						Params: xdr.LiquidityPoolConstantProductParameters{
							AssetA: lpAssetA,
							AssetB: lpAssetB,
							Fee:    30,
						},
						ReserveA:                 105,
						ReserveB:                 10,
						TotalPoolShares:          35,
						PoolSharesTrustLineCount: 5,
					},
				},
			},
		},
	}
	return ingest.Change{
		Type: xdr.LedgerEntryTypeLiquidityPool,
		Pre:  &ledgerEntry,
		Post: nil,
	}
}

func makePoolTestOutput() PoolOutput {
	return PoolOutput{
		PoolID:             "172d430000000000000000000000000000000000000000000000000000000000",
		PoolType:           "constant_product",
		PoolFee:            30,
		TrustlineCount:     5,
		PoolShareCount:     0.0000035,
		AssetAType:         "native",
		AssetACode:         lpAssetA.GetCode(),
		AssetAIssuer:       lpAssetA.GetIssuer(),
		AssetAID:           -5706705804583548011,
		AssetAReserve:      0.0000105,
		AssetBType:         "credit_alphanum4",
		AssetBCode:         lpAssetB.GetCode(),
		AssetBID:           -9138108998176092786,
		AssetBIssuer:       lpAssetB.GetIssuer(),
		AssetBReserve:      0.0000010,
		LastModifiedLedger: 30705278,
		LedgerEntryChange:  2,
		Deleted:            true,
	}
}
