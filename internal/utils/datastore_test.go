package utils

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildDataStoreConfigGCS(t *testing.T) {
	env := EnvironmentDetails{
		Network: "pubnet",
		CommonFlagValues: CommonFlagValues{
			DatastorePath: "sdf-ledger-close-meta/v1/ledgers",
			DatastoreType: "GCS",
		},
	}

	config, err := BuildDataStoreConfig(env)
	require.NoError(t, err)
	assert.Equal(t, "GCS", config.Type)
	assert.Equal(t, map[string]string{
		"destination_bucket_path": "sdf-ledger-close-meta/v1/ledgers/pubnet",
	}, config.Params)
	assert.Equal(t, datastore.DataStoreSchema{
		LedgersPerFile:    1,
		FilesPerPartition: 64000,
	}, config.Schema)
}

func TestBuildDataStoreConfigS3LowercaseWithoutEndpoint(t *testing.T) {
	env := EnvironmentDetails{
		Network: "pubnet",
		CommonFlagValues: CommonFlagValues{
			DatastorePath:   "aws-public-blockchain/v1.1/stellar/ledgers",
			DatastoreType:   "s3",
			DatastoreRegion: "us-east-2",
		},
	}

	config, err := BuildDataStoreConfig(env)
	require.NoError(t, err)
	assert.Equal(t, "S3", config.Type)
	assert.Equal(t, map[string]string{
		"destination_bucket_path": "aws-public-blockchain/v1.1/stellar/ledgers/pubnet",
		"region":                  "us-east-2",
	}, config.Params)
}

func TestBuildDataStoreConfigS3WithEndpoint(t *testing.T) {
	env := EnvironmentDetails{
		Network: "testnet",
		CommonFlagValues: CommonFlagValues{
			DatastorePath:        "aws-public-blockchain/v1.1/stellar/ledgers",
			DatastoreType:        "S3",
			DatastoreRegion:      "us-east-2",
			DatastoreEndpointURL: "https://s3.us-east-2.amazonaws.com",
		},
	}

	config, err := BuildDataStoreConfig(env)
	require.NoError(t, err)
	assert.Equal(t, "S3", config.Type)
	assert.Equal(t, map[string]string{
		"destination_bucket_path": "aws-public-blockchain/v1.1/stellar/ledgers/testnet",
		"region":                  "us-east-2",
		"endpoint_url":            "https://s3.us-east-2.amazonaws.com",
	}, config.Params)
}

func TestBuildDataStoreConfigS3MissingRegion(t *testing.T) {
	env := EnvironmentDetails{
		Network: "pubnet",
		CommonFlagValues: CommonFlagValues{
			DatastorePath: "aws-public-blockchain/v1.1/stellar/ledgers",
			DatastoreType: "S3",
		},
	}

	_, err := BuildDataStoreConfig(env)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--datastore-region")
}

func TestBuildDataStoreConfigUnknownType(t *testing.T) {
	env := EnvironmentDetails{
		Network: "pubnet",
		CommonFlagValues: CommonFlagValues{
			DatastorePath: "sdf-ledger-close-meta/v1/ledgers",
			DatastoreType: "filesystem",
		},
	}

	_, err := BuildDataStoreConfig(env)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "filesystem")
	assert.Contains(t, err.Error(), "GCS")
	assert.Contains(t, err.Error(), "S3")
}

func TestBuildDataStoreConfigEmptyTypeDefaultsToGCS(t *testing.T) {
	env := EnvironmentDetails{
		Network: "futurenet",
		CommonFlagValues: CommonFlagValues{
			DatastorePath: "sdf-ledger-close-meta/v1/ledgers",
		},
	}

	config, err := BuildDataStoreConfig(env)
	require.NoError(t, err)
	assert.Equal(t, "GCS", config.Type)
	assert.Equal(t, map[string]string{
		"destination_bucket_path": "sdf-ledger-close-meta/v1/ledgers/futurenet",
	}, config.Params)
}

func TestAddCommonFlagsDatastoreDefaults(t *testing.T) {
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
