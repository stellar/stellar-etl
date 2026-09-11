package utils

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
)

// envWith returns EnvironmentDetails carrying the given common flag values with
// a fixed network so the destination_bucket_path is deterministic.
func envWith(common CommonFlagValues) EnvironmentDetails {
	return EnvironmentDetails{
		Network:          "pubnet",
		CommonFlagValues: common,
	}
}

func TestBuildDatastoreConfig_GCSDefault(t *testing.T) {
	env := envWith(CommonFlagValues{
		DatastorePath: "sdf-ledger-close-meta/v1/ledgers",
		DatastoreType: "GCS",
	})

	config, err := BuildDatastoreConfig(env)
	assert.NoError(t, err)
	assert.Equal(t, "GCS", config.Type)
	assert.Equal(t, "sdf-ledger-close-meta/v1/ledgers/pubnet", config.Params["destination_bucket_path"])
	_, hasRegion := config.Params["region"]
	assert.False(t, hasRegion, "GCS config should not carry a region")
	_, hasEndpoint := config.Params["endpoint_url"]
	assert.False(t, hasEndpoint, "GCS config should not carry an endpoint_url")
	assert.Equal(t, uint32(1), config.Schema.LedgersPerFile)
	assert.Equal(t, uint32(64000), config.Schema.FilesPerPartition)
}

func TestBuildDatastoreConfig_S3LowercaseWithRegionNoEndpoint(t *testing.T) {
	env := envWith(CommonFlagValues{
		DatastorePath:   "aws-public-blockchain/v1.1/stellar/ledgers",
		DatastoreType:   "s3",
		DatastoreRegion: "us-east-2",
	})

	config, err := BuildDatastoreConfig(env)
	assert.NoError(t, err)
	assert.Equal(t, "S3", config.Type)
	assert.Equal(t, "aws-public-blockchain/v1.1/stellar/ledgers/pubnet", config.Params["destination_bucket_path"])
	assert.Equal(t, "us-east-2", config.Params["region"])
	_, hasEndpoint := config.Params["endpoint_url"]
	assert.False(t, hasEndpoint, "endpoint_url should be absent when not provided")
}

func TestBuildDatastoreConfig_S3WithEndpoint(t *testing.T) {
	env := envWith(CommonFlagValues{
		DatastorePath:        "my-bucket/ledgers",
		DatastoreType:        "S3",
		DatastoreRegion:      "us-west-2",
		DatastoreEndpointURL: "https://s3.example.com",
	})

	config, err := BuildDatastoreConfig(env)
	assert.NoError(t, err)
	assert.Equal(t, "S3", config.Type)
	assert.Equal(t, "us-west-2", config.Params["region"])
	assert.Equal(t, "https://s3.example.com", config.Params["endpoint_url"])
}

func TestBuildDatastoreConfig_S3MissingRegion(t *testing.T) {
	env := envWith(CommonFlagValues{
		DatastorePath: "my-bucket/ledgers",
		DatastoreType: "S3",
	})

	_, err := BuildDatastoreConfig(env)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "--datastore-region")
}

func TestBuildDatastoreConfig_UnknownType(t *testing.T) {
	env := envWith(CommonFlagValues{
		DatastorePath: "my-bucket/ledgers",
		DatastoreType: "azure",
	})

	_, err := BuildDatastoreConfig(env)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "azure")
	assert.Contains(t, err.Error(), "GCS")
	assert.Contains(t, err.Error(), "S3")
}

func TestBuildDatastoreConfig_EmptyTypeDefaultsToGCS(t *testing.T) {
	env := envWith(CommonFlagValues{
		DatastorePath: "sdf-ledger-close-meta/v1/ledgers",
		DatastoreType: "",
	})

	config, err := BuildDatastoreConfig(env)
	assert.NoError(t, err)
	assert.Equal(t, "GCS", config.Type)
	assert.Equal(t, "sdf-ledger-close-meta/v1/ledgers/pubnet", config.Params["destination_bucket_path"])
}

func TestAddCommonFlags_DatastoreFlagDefaults(t *testing.T) {
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	AddCommonFlags(fs)

	assert.NoError(t, fs.Parse(nil))

	datastoreType, err := fs.GetString("datastore-type")
	assert.NoError(t, err)
	assert.Equal(t, "GCS", datastoreType)

	datastoreRegion, err := fs.GetString("datastore-region")
	assert.NoError(t, err)
	assert.Equal(t, "", datastoreRegion)

	datastoreEndpointURL, err := fs.GetString("datastore-endpoint-url")
	assert.NoError(t, err)
	assert.Equal(t, "", datastoreEndpointURL)
}
