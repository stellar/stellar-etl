package utils

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"

	"cloud.google.com/go/storage"

	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/xdr"
)


type GCSBackend struct {
	bucket  *storage.BucketHandle
}

func (b GCSBackend) GetLatestLedgerSequence(ctx context.Context) (uint32, error) {
	r, err := b.bucket.Object("latest").NewReader(context.Background())
	if err != nil {
		return 0, err
	}
	defer r.Close()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		return 0, err
	}
	if parsed, err := strconv.ParseUint(buf.String(), 10, 32); err != nil {
		return 0, err
	} else {
		return uint32(parsed), nil
	}
}

func (b GCSBackend) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, error) {
	var ledger xdr.LedgerCloseMeta
	r, err := b.bucket.Object("ledgers/"+strconv.FormatUint(uint64(sequence), 10)).NewReader(context.Background())
	if err != nil {
		return ledger, err
	}
	defer r.Close()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		return ledger, err
	}
	if err = ledger.UnmarshalBinary(buf.Bytes()); err != nil {
		return ledger, err
	}

	return ledger, nil
}

func (b GCSBackend) PrepareRange(ctx context.Context, ledgerRange ledgerbackend.Range) error {
	return nil
}

func (b GCSBackend) IsPrepared(ctx context.Context, ledgerRange ledgerbackend.Range) (bool, error) {
	return true, nil
}

func (b GCSBackend) Close() error {
	return b.Close()
}

func CreateGCSBackend(gcsCredentials, bucketName string, start, end uint32) (ledgerbackend.LedgerBackend, error) {
	if len(gcsCredentials) > 0 {
		os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", gcsCredentials)
		fmt.Printf("Using credentials found at: %s\n", gcsCredentials)
	}

	client, err := storage.NewClient(context.Background())
	if err != nil {
		return nil, err
	}

	backend := GCSBackend{bucket: client.Bucket(bucketName)}
	latestLedger, err := backend.GetLatestLedgerSequence(context.Background())
	if err != nil {
		return nil, err
	}

	if err = ValidateLedgerRange(start, end, latestLedger); err != nil {
		return nil, err
	}
	return backend, nil
}