package transform

import (
	"fmt"
	"io"
	"testing"

	ingestio "github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/network"
	"github.com/stellar/stellar-etl/internal/utils"
)

func TestTransformOnTrueTransaction(t *testing.T) {
	backend := createTestArchiveBackend()
	sequence := uint32(30579396) //30578981
	txReader, err := ingestio.NewLedgerTransactionReader(backend, network.PublicNetworkPassphrase, sequence)
	defer txReader.Close()
	utils.PanicOnError(err)
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		utils.PanicOnError(err)
		convertedTransaction, err := ConvertTransaction(tx, txReader.GetHeader())
		utils.PanicOnError(err)
		fmt.Println(convertedTransaction.CreatedAt.String())
	}
}
