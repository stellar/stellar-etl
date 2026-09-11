package utils

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildDatastoreConfig_GCSDefault(t *testing.T) {
	env := EnvironmentDetails{
		Network: "pubnet",
		CommonFlagValues: CommonFlagValues{
			DatastorePath: "sdf-ledger-close-meta/v1/ledgers",
			DatastoreType: "GCS",
		},
	}

	cfg, err := BuildDatastoreConfig(env)
	require.NoError(t, err)
	assert.Equal(t, "GCS", cfg.Type)
	assert.Equal(t, map[string]string{
		"destination_bucket_path": "sdf-ledger-close-meta/v1/ledgers/pubnet",
	}, cfg.Params)
	assert.Equal(t, datastore.DataStoreSchema{
		LedgersPerFile:    1,
		FilesPerPartition: 64000,
	}, cfg.Schema)
}

func TestBuildDatastoreConfig_S3LowercaseWithRegionWithoutEndpoint(t *testing.T) {
	env := EnvironmentDetails{
		Network: "testnet",
		CommonFlagValues: CommonFlagValues{
			DatastorePath:   "aws-public-blockchain/v1.1/stellar/ledgers",
			DatastoreType:   "s3",
			DatastoreRegion: "us-east-2",
		},
	}

	cfg, err := BuildDatastoreConfig(env)
	require.NoError(t, err)
	assert.Equal(t, "S3", cfg.Type)
	assert.Equal(t, map[string]string{
		"destination_bucket_path": "aws-public-blockchain/v1.1/stellar/ledgers/testnet",
		"region":                  "us-east-2",
	}, cfg.Params)
}

func TestBuildDatastoreConfig_S3WithEndpoint(t *testing.T) {
	env := EnvironmentDetails{
		Network: "pubnet",
		CommonFlagValues: CommonFlagValues{
			DatastorePath:        "bucket/prefix",
			DatastoreType:        "S3",
			DatastoreRegion:      "us-east-2",
			DatastoreEndpointURL: "https://s3.us-east-2.amazonaws.com",
		},
	}

	cfg, err := BuildDatastoreConfig(env)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"destination_bucket_path": "bucket/prefix/pubnet",
		"region":                  "us-east-2",
		"endpoint_url":            "https://s3.us-east-2.amazonaws.com",
	}, cfg.Params)
}

func TestBuildDatastoreConfig_S3MissingRegion(t *testing.T) {
	env := EnvironmentDetails{
		Network: "pubnet",
		CommonFlagValues: CommonFlagValues{
			DatastorePath: "bucket/prefix",
			DatastoreType: "S3",
		},
	}

	_, err := BuildDatastoreConfig(env)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--datastore-region")
}

func TestBuildDatastoreConfig_UnknownType(t *testing.T) {
	env := EnvironmentDetails{
		Network: "pubnet",
		CommonFlagValues: CommonFlagValues{
			DatastorePath: "bucket/prefix",
			DatastoreType: "azure",
		},
	}

	_, err := BuildDatastoreConfig(env)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "azure")
	assert.Contains(t, err.Error(), "GCS")
	assert.Contains(t, err.Error(), "S3")
}

func TestBuildDatastoreConfig_EmptyTypeDefaultsToGCS(t *testing.T) {
	env := EnvironmentDetails{
		Network: "pubnet",
		CommonFlagValues: CommonFlagValues{
			DatastorePath: "bucket/prefix",
		},
	}

	cfg, err := BuildDatastoreConfig(env)
	require.NoError(t, err)
	assert.Equal(t, "GCS", cfg.Type)
	assert.Equal(t, map[string]string{
		"destination_bucket_path": "bucket/prefix/pubnet",
	}, cfg.Params)
}

func TestAddCommonFlags_DatastoreDefaults(t *testing.T) {
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	AddCommonFlags(fs)

	require.NoError(t, fs.Parse(nil))

	datastoreType, err := fs.GetString("datastore-type")
	require.NoError(t, err)
	assert.Equal(t, "GCS", datastoreType)

	datastoreRegion, err := fs.GetString("datastore-region")
	require.NoError(t, err)
	assert.Equal(t, "", datastoreRegion)

	datastoreEndpointURL, err := fs.GetString("datastore-endpoint-url")
	require.NoError(t, err)
	assert.Equal(t, "", datastoreEndpointURL)
}
