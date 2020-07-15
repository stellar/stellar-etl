package transform

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/gdexlab/go-render/render"

	"github.com/stellar/go/exp/ingest/ledgerbackend"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
)

func createTestArchiveBackend() *ledgerbackend.HistoryArchiveBackend {
	archiveStellarURL := "http://history.stellar.org/prd/core-live/core_live_001"
	backend, err := ledgerbackend.NewHistoryArchiveBackendFromURL(archiveStellarURL)
	utils.PanicOnError(err)
	return backend
}
func TestTransformOnTrueLedger(t *testing.T) {
	sequenceNumber := uint32(30511378)
	backend := createTestArchiveBackend()
	defer backend.Close()

	ok, testLedger, err := backend.GetLedger(sequenceNumber)
	utils.PanicOnError(err)
	if !ok {
		panic("Ledger does not exist")
	}
	encodedLedgerHeader := "AAAADQV9uRTeVDxeos9csdoGFP4EVPC7fjwdREAdvbJGK3Mdlf53swrayxtj+WanhAsXUxXGrlWjA24P1WYTEHqDNscAAAAAXwXKJgAAAAAAAAAAfA3y3e25icQxUH/+uuK927qF7CA80UGOyyxaUZXw30NQfVdKEsLSPrKyXtQDHS9r2bBx0JYnj5m9aELaByqf4gHRkRIOoh6z7HlbYQAAEII3mx+pAAABFgAAAAAPCrxaAAAAZABMS0AAAAPob1lZHuPT2yeYW7x4Zs+KV0kGxc28wQEbbCIVQefXPPozBfmto7Eki4cSFF1t4R7PTUInwbXia1D32UdH49065HgU8x0XGBQSpM2IDZnned+TWHn8dogc71zlLocnXFhMTCIWC0Qiq8Pzem7s3VeReXI7rAo/U1ze1hS2OJ3voNkAAAAA"
	byteFormOfLedgerHeader := []byte(encodedLedgerHeader)
	parsedTime, err := time.Parse("2006-1-2 15:04:05 MST", "2020-07-08 13:29:10 UTC")
	desiredOutput := LedgerOutput{
		Sequence:           int32(sequenceNumber),
		LedgerHash:         "6898f0f7b4945f47152aa697eaf79b3df7ddaa7d1a3c697e28197ad1feab7b0e",
		PreviousLedgerHash: "057db914de543c5ea2cf5cb1da0614fe0454f0bb7e3c1d44401dbdb2462b731d",
		LedgerHeader:       byteFormOfLedgerHeader,

		TransactionCount:           10,
		OperationCount:             12,
		SuccessfulTransactionCount: 10,
		FailedTransactionCount:     1,
		TxSetOperationCount:        "13",

		ClosedAt: parsedTime,

		TotalCoins:      1054439020873472865,
		FeePool:         18151464705961,
		BaseFee:         100,
		BaseReserve:     5000000,
		MaxTxSetSize:    1000,
		ProtocolVersion: 13,
	}
	inputLedgerPretty := render.AsCode(testLedger)
	fmt.Println(inputLedgerPretty)
	convertedLedger, err := TransformLedger(testLedger)
	assert.Equal(t, desiredOutput, convertedLedger)

}

func TestTransformLedger(t *testing.T) {
	type transformTest struct {
		input      xdr.LedgerCloseMeta
		wantOutput LedgerOutput
		wantErr    error
	}
	/*encodedLedgerHeader := "AAAADfY8FdDq9Ir711GkxN+t5Uo0SAU8R8WnHWImaK4MwqIIkvQSArXhiHcF4mc5dW+oX7J45SX+kVIw1fZFQOgAGUEAAAAAXwtt4wAAAAAAAAAAglgbxuZna3ZZPDE9cmMZZu4hr7EKrE9oMaGRPu2QqnhEVa9EjsMjpKJnOr9UnDpix+BSese+YEEnMYBllAoQYAHSmSUOoh6z7HlbYQAAEILAyUaJAAABFgAAAAAPNsPXAAAAZABMS0AAAAPomTlchfG4kTZ/6o0otCLK44Twu6KISQjni2rUp4dG8r+uRv1ZyQAYHHxS1EDGrx4+4hJAXwwfQZIiQ02F7J/5e/MWwpi1ImmLz+kwObksbcGQNVo7wwrC65BaoDpSpMXAeBTzHRcYFBKkzYgNmed535NYefx2iBzvXOUuhydcWEwAAAAA"
	byteFormOfLedgerHeader := []byte(encodedLedgerHeader)
	parsedTime, err := time.Parse("2006-1-2 15:04:05 MST", "2020-07-12 20:09:07 UTC")*/
	tests := []transformTest{
		{
			wrapLedgerHeader(xdr.LedgerHeader{
				TotalCoins: -1,
			}),
			LedgerOutput{},
			errors.New("The total number of coins is a negative value"),
		},
		{
			wrapLedgerHeader(xdr.LedgerHeader{
				FeePool: -1,
			}),
			LedgerOutput{},
			errors.New("The fee pool is a negative value"),
		},
		{
			wrapLedgerHeaderWithTransactions(xdr.LedgerHeader{
				MaxTxSetSize: 0,
			}, 2),
			LedgerOutput{},
			errors.New("The number of transactions and results are different"),
		},
		/*{

			LedgerOutput{
				Sequence:           int32(30578981),
				LedgerHash:         "26932dc4d84b5fabe9ae744cb43ce4c6daccf98c86a991b2a14945b1adac4d59",
				PreviousLedgerHash: "f63c15d0eaf48afbd751a4c4dfade54a3448053c47c5a71d622668ae0cc2a208",
				LedgerHeader:       byteFormOfLedgerHeader,

				TransactionCount:           2,
				OperationCount:             6,
				SuccessfulTransactionCount: 2,
				FailedTransactionCount:     201,
				TxSetOperationCount:        "208",

				ClosedAt: parsedTime,

				TotalCoins:      1054439020873472865,
				FeePool:         18153766209161,
				BaseFee:         100,
				BaseReserve:     5000000,
				MaxTxSetSize:    1000,
				ProtocolVersion: 13,
			},
			nil,
		}*/
	}

	for _, test := range tests {
		actualOutput, actualError := TransformLedger(test.input)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func wrapLedgerHeaderWithTransactions(header xdr.LedgerHeader, numTransactions int) xdr.LedgerCloseMeta {
	transactionEnvelopes := []xdr.TransactionEnvelope{}
	for txNum := 0; txNum < numTransactions; txNum++ {
		transactionEnvelopes = append(transactionEnvelopes, createSampleTx(uint32(txNum)))
	}
	lcm, err := xdr.NewLedgerCloseMeta(0, xdr.LedgerCloseMetaV0{
		LedgerHeader: xdr.LedgerHeaderHistoryEntry{
			Header: header,
		},
		TxSet: xdr.TransactionSet{Txs: transactionEnvelopes},
	})
	utils.PanicOnError(err)
	return lcm
}

func wrapLedgerHeader(header xdr.LedgerHeader) xdr.LedgerCloseMeta {
	lcm, err := xdr.NewLedgerCloseMeta(0, xdr.LedgerCloseMetaV0{
		LedgerHeader: xdr.LedgerHeaderHistoryEntry{
			Header: header,
		}})
	utils.PanicOnError(err)
	return lcm
}

func createSampleTx(sequence uint32) xdr.TransactionEnvelope {
	kp, err := keypair.Random()
	utils.PanicOnError(err)

	sourceAccount := txnbuild.NewSimpleAccount(kp.Address(), int64(0))
	tx, err := txnbuild.NewTransaction(
		txnbuild.TransactionParams{
			SourceAccount: &sourceAccount,
			Operations: []txnbuild.Operation{
				&txnbuild.BumpSequence{
					BumpTo: int64(sequence),
				},
			},
			BaseFee:    txnbuild.MinBaseFee,
			Timebounds: txnbuild.NewInfiniteTimeout(),
		},
	)
	utils.PanicOnError(err)

	env, err := tx.TxEnvelope()
	utils.PanicOnError(err)
	return env
}
