# stage 1: build stellar-etl app
# golang 1.19, pinned by sha digest
#FROM golang@sha256:04f76f956e51797a44847e066bde1341c01e09054d3878ae88c7f77f09897c4d AS build
FROM golang:1.20.5-buster AS build

WORKDIR /usr/src/etl

# pre-copy/cache go.mod for pre-downloading dependencies and only redownloading them in subsequent builds if they change
COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .
RUN go build -v -o /usr/local/bin ./...

# stage 2: runtime enviroment
# stellar/stellar-core 19, pinned by sha digest
# futurenet preview 10 core image
FROM stellar/stellar-core:19.13.1-1481.3acf6dd26.focal
# image for testnet release
#FROM chowbao/stellar-core:19.13.1-1434.35170ec6c.focal-soroban

WORKDIR /etl

COPY --from=build /usr/local/bin/stellar-etl /usr/local/bin/stellar-etl
COPY --from=build /usr/src/etl/docker docker

# clear entrypoint from stellar-core image
ENTRYPOINT []

CMD ["stellar-etl"]
