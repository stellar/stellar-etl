# stage 1: build stellar-etl app
FROM golang:1.22.1-alpine AS build

WORKDIR /usr/src/etl

# pre-copy/cache go.mod for pre-downloading dependencies and only redownloading them in subsequent builds if they change
COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .
RUN go build -v -o /usr/local/bin ./...

# stage 2: runtime enviroment
FROM stellar/stellar-core:20.2.0-1716.rc3.34d82fc00.focal

WORKDIR /etl

COPY --from=build /usr/local/bin/stellar-etl /usr/local/bin/stellar-etl
COPY --from=build /usr/src/etl/docker docker

# clear entrypoint from stellar-core image
ENTRYPOINT []

CMD ["stellar-etl"]
