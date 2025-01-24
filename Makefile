# Check if we need to prepend docker commands with sudo
SUDO := $(shell docker version >/dev/null 2>&1 || echo "sudo")

# https://github.com/opencontainers/image-spec/blob/master/annotations.md
BUILD_DATE := $(shell date -u +%FT%TZ)

DEFAULT_ETLHASH := stellar/stellar-etl:$(shell git rev-parse --short=9 HEAD)
ETLHASH ?= $(DEFAULT_ETLHASH)

# Build rust xdr2json
CARGO_BUILD_TARGET ?= $(shell rustc -vV | sed -n 's|host: ||p')

# update the Cargo.lock every time the Cargo.toml changes.
Cargo.lock: Cargo.toml
	cargo update --workspace

build-libs: Cargo.lock
	cd lib/xdr2json && \
	cargo build --target $(CARGO_BUILD_TARGET) --profile release-with-panic-unwind

docker-build:
	$(SUDO) docker build --platform linux/amd64 --pull --no-cache --label org.opencontainers.image.created="$(BUILD_DATE)" \
	-t $(ETLHASH) -t stellar/stellar-etl:latest -f ./docker/Dockerfile .

docker-push:
	$(SUDO) docker push $(ETLHASH)
	$(SUDO) docker push stellar/stellar-etl:latest

int-test:
	docker-compose build
	docker-compose run \
	-v $(HOME)/.config/gcloud/application_default_credentials.json:/usr/credential.json:ro \
	-v $(PWD)/testdata:/usr/src/etl/testdata \
	-e GOOGLE_APPLICATION_CREDENTIALS=/usr/credential.json \
	integration-tests \
	go test -v ./cmd -timeout 30m

int-test-update:
	docker-compose build
	docker-compose run \
	-v $(HOME)/.config/gcloud/application_default_credentials.json:/usr/credential.json:ro \
	-v $(PWD)/testdata:/usr/src/etl/testdata \
	-e GOOGLE_APPLICATION_CREDENTIALS=/usr/credential.json \
	integration-tests \
	go test -v ./cmd -timeout 30m -args -update=true

lint:
	pre-commit run --show-diff-on-failure --color=always --all-files
