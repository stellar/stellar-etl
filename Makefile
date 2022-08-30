# Check if we need to prepend docker commands with sudo
SUDO := $(shell docker version >/dev/null 2>&1 || echo "sudo")

# https://github.com/opencontainers/image-spec/blob/master/annotations.md
BUILD_DATE := $(shell date -u +%FT%TZ)

ETLHASH=stellar/stellar-etl:$(shell git rev-parse --short HEAD)

docker-build:
	$(SUDO) docker build --pull --no-cache --label org.opencontainers.image.created="$(BUILD_DATE)" \
	-t $(ETLHASH) -t stellar/stellar-etl:latest -f ./docker/Dockerfile .

docker-push:
	$(SUDO) docker push $(ETLHASH)
	$(SUDO) docker push stellar/stellar-etl:latest
