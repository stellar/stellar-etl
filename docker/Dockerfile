# stage 1: build stellar-etl app
FROM golang:1.23.4-alpine AS build

WORKDIR /usr/src/etl

# pre-copy/cache go.mod for pre-downloading dependencies and only redownloading them in subsequent builds if they change
COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .
RUN go build -v -o /usr/local/bin ./...

# stage 2: runtime enviroment
FROM stellar/unsafe-stellar-core:22.0.0-2126.rc3.92923c2db.focal

WORKDIR /etl

COPY --from=build /usr/local/bin/stellar-etl /usr/local/bin/stellar-etl
COPY --from=build /usr/src/etl/docker docker

# changing workdir to a new path in order to use mounted empty ephemeral volumes as storage
WORKDIR /etl/data

# clear entrypoint from stellar-core image
ENTRYPOINT []

CMD ["stellar-etl"]
