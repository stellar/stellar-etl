package transform

import (
	"fmt"
	"testing"

	"github.com/stellar/go/exp/ingest/ledgerbackend"
)

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

func createTestArchiveBackend() *ledgerbackend.HistoryArchiveBackend {
	archiveStellarURL := "http://history.stellar.org/prd/core-live/core_live_001"
	backend, err := ledgerbackend.NewHistoryArchiveBackendFromURL(archiveStellarURL)
	checkError(err)
	return backend
}
func TestLedgerTransform(t *testing.T) {
	backend := createTestArchiveBackend()
	defer backend.Close()

	ok, testLedger, err := backend.GetLedger(30594039)
	checkError(err)
	if !ok {
		panic("Ledger does not exist")
	}
	convertedLedger := ConvertLedger(testLedger)
	fmt.Println(convertedLedger)
}
