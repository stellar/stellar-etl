# Check if we need to prepend docker commands with sudo
SUDO := $(shell docker version >/dev/null 2>&1 || echo "sudo")


ETLHASH=stellar/stellar-etl:$(shell git rev-parse --short HEAD)

docker-build:
	$(SUDO) docker build --no-cache -t $(ETLHASH) -t stellar/stellar-etl:latest ./docker

docker-push:
	$(SUDO) docker push $(ETLHASH)
	$(SUOD) docker push stellar/stellar-etl:latest