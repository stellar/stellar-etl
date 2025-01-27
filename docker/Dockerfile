# stage 1: build the rust xdr2json
FROM rust:1.84.0-bullseye AS rust-build

WORKDIR /usr/src/etl

COPY . .
RUN cargo update --workspace
RUN rustup target add x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu
RUN cd lib/xdr2json && cargo build --target x86_64-unknown-linux-gnu --profile release-with-panic-unwind

# stage 2: build stellar-etl app
FROM golang:1.22.1-bullseye AS go-build

WORKDIR /etl

# pre-copy/cache go.mod for pre-downloading dependencies and only redownloading them in subsequent builds if they change
COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .
COPY --from=rust-build /usr/src/etl/target ./target

RUN go build -v -o /usr/local/bin ./...

# changing workdir to a new path in order to use mounted empty ephemeral volumes as storage
WORKDIR /etl/data

# clear entrypoint from stellar-core image
ENTRYPOINT []

CMD ["stellar-etl"]
