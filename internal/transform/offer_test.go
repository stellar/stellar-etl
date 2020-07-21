package transform

import (
	"context"
	"fmt"
	"io"
	"testing"

	ingestio "github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/utils"
)

func TestLive(t *testing.T) {
	//30715263 has deleted stuff
	sequence := uint32(30715263) //30522111, 30522175, 30715263, 30715199
	archiveURL := "http://history.stellar.org/prd/core-live/core_live_001"
	archive, err := historyarchive.Connect(
		archiveURL,
		historyarchive.ConnectOptions{Context: context.Background()},
	)
	utils.PanicOnError(err)
	/*for s := 30715281; s > 0; s-- {
		_, err := ingestio.MakeSingleLedgerStateReader(context.Background(), archive, uint32(s))
		if err == nil {
			fmt.Println(s)
		} else if s%1000 == 0 {
			fmt.Printf("not working %d\n", s)
		}
	}*/
	stateReader, err := ingestio.MakeSingleLedgerStateReader(context.Background(), archive, sequence)
	utils.PanicOnError(err)
	for {
		change, err := stateReader.Read()
		if err == io.EOF {
			break
		}
		utils.PanicOnError(err)
		if change.Type == xdr.LedgerEntryTypeOffer {
			if change.LedgerEntryChangeType() == xdr.LedgerEntryChangeTypeLedgerEntryRemoved {
				fmt.Println("deleted")
			}
			converted, err := TransformOffer(change)

			if converted.OfferID == 260678415 {
				fmt.Println("deleted")
			}
			utils.PanicOnError(err)
		}

	}
}
