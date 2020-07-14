package transform

import (
	"errors"
	"testing"
	"time"

	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/go/exp/ingest/ledgerbackend"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
)

func createTestArchiveBackend() *ledgerbackend.HistoryArchiveBackend {
	archiveStellarURL := "http://history.stellar.org/prd/core-live/core_live_001"
	backend, err := ledgerbackend.NewHistoryArchiveBackendFromURL(archiveStellarURL)
	utils.PanicOnError(err)
	return backend
}
func TestTransformOnTrueLedger(t *testing.T) {
	sequenceNumber := uint32(30578981)
	backend := createTestArchiveBackend()
	defer backend.Close()

	ok, testLedger, err := backend.GetLedger(sequenceNumber)
	utils.PanicOnError(err)
	if !ok {
		panic("Ledger does not exist")
	}
	encodedLedgerHeader := "AAAADfY8FdDq9Ir711GkxN+t5Uo0SAU8R8WnHWImaK4MwqIIkvQSArXhiHcF4mc5dW+oX7J45SX+kVIw1fZFQOgAGUEAAAAAXwtt4wAAAAAAAAAAglgbxuZna3ZZPDE9cmMZZu4hr7EKrE9oMaGRPu2QqnhEVa9EjsMjpKJnOr9UnDpix+BSese+YEEnMYBllAoQYAHSmSUOoh6z7HlbYQAAEILAyUaJAAABFgAAAAAPNsPXAAAAZABMS0AAAAPomTlchfG4kTZ/6o0otCLK44Twu6KISQjni2rUp4dG8r+uRv1ZyQAYHHxS1EDGrx4+4hJAXwwfQZIiQ02F7J/5e/MWwpi1ImmLz+kwObksbcGQNVo7wwrC65BaoDpSpMXAeBTzHRcYFBKkzYgNmed535NYefx2iBzvXOUuhydcWEwAAAAA"
	byteFormOfLedgerHeader := []byte(encodedLedgerHeader)
	parsedTime, err := time.Parse("2006-1-2 15:04:05 MST", "2020-07-12 20:09:07 UTC")
	desiredOutput := LedgerOutput{
		Sequence:           int32(sequenceNumber),
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
	}
	convertedLedger, err := ConvertLedger(testLedger)
	assert.Equal(t, desiredOutput, convertedLedger)

}

func TestTransformLedger(t *testing.T) {
	type transformTest struct {
		input         xdr.LedgerCloseMeta
		desiredOutput LedgerOutput
		desiredError  error
	}

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
	}

	for _, test := range tests {
		actualOutput, actualError := ConvertLedger(test.input)
		assert.Equal(t, test.desiredError, actualError)
		assert.Equal(t, test.desiredOutput, actualOutput)
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
	if err != nil {
		panic(err)
	}

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
	if err != nil {
		panic(err)
	}

	env, err := tx.TxEnvelope()
	if err != nil {
		panic(err)
	}
	return env
}
