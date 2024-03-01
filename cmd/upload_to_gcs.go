package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"cloud.google.com/go/storage"
)

type GCS struct {
	gcsCredentialsPath string
	gcsBucket          string
}

func newGCS(gcsCredentialsPath, gcsBucket string) CloudStorage {
	return &GCS{
		gcsCredentialsPath: gcsCredentialsPath,
		gcsBucket:          gcsBucket,
	}
}

func (g *GCS) UploadTo(credentialsPath, bucket, path string) error {
	// Use credentials file in dev/local runs. Otherwise, derive credentials from the service account.
	if len(credentialsPath) > 0 {
		os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credentialsPath)
		cmdLogger.Infof("Using credentials found at: %s", credentialsPath)
	}

	reader, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %v", path, err)
	}

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Hour)
	defer cancel()

	wc := client.Bucket(bucket).Object(path).NewWriter(ctx)

	uploadLocation := fmt.Sprintf("gs://%s/%s", bucket, path)
	cmdLogger.Infof("Uploading %s to %s", path, uploadLocation)

	var written int64
	if written, err = io.Copy(wc, reader); err != nil {
		return fmt.Errorf("unable to copy: %v", err)
	}
	err = wc.Close()
	if err != nil {
		return err
	}

	cmdLogger.Infof("Successfully uploaded %d bytes to gs://%s/%s", written, bucket, path)

	deleteLocalFiles(path)

	return nil
}
