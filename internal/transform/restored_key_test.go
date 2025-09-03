package transform

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

func TestTransformRestoredKey(t *testing.T) {
	type inputStruct struct {
		ingest ingest.Change
	}
	type transformTest struct {
		input      inputStruct
		wantOutput RestoredKeyOutput
		wantErr    error
	}

	hardCodedInput, err := makeRestoredKeyTestInput()
	assert.NoError(t, err)
	hardCodedOutput := makeRestoredKeyTestOutput()

	tests := []transformTest{
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
		actualOutput, actualError := TransformRestoredKey(test.input.ingest, header)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func makeRestoredKeyTestInput() (ledgerChange ingest.Change, err error) {
	ledgerChange = ingest.Change{
		ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryRestored,
		Type:       xdr.LedgerEntryTypeOffer,
		Pre:        nil,
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
			Ext: xdr.LedgerEntryExt{
				V: 1,
				V1: &xdr.LedgerEntryExtensionV1{
					SponsoringId: &testAccount3ID,
				},
			},
		},
	}
	return
}

func makeRestoredKeyTestOutput() RestoredKeyOutput {
	return RestoredKeyOutput{
		LedgerKeyHash:      "AAAAAgAAAACI4aa0pXFSj6qfJuIObLw/5zyugLRGYwxb7wFSr3B9eAAAAAAPiaMn",
		LedgerEntryType:    "LedgerEntryTypeOffer",
		LastModifiedLedger: 30715263,
		LedgerSequence:     10,
		ClosedAt:           time.Date(1970, time.January, 1, 0, 16, 40, 0, time.UTC),
	}
}
