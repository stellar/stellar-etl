# docker image stellar/stellar-core version 19, pinned by sha digest
FROM stellar/stellar-core@sha256:b4e85991ea5a72667a147e2d49018269c252c096dfc2a845d13142e615ea4dd3

WORKDIR /tmp

# download, validate and extract go binaries
RUN wget https://go.dev/dl/go1.18.5.linux-amd64.tar.gz
RUN echo '9e5de37f9c49942c601b191ac5fba404b868bfc21d446d6960acc12283d6e5f2 go1.18.5.linux-amd64.tar.gz' > go1.18.5.linux-amd64.tar.gz.checksum
RUN sha256sum -c go1.18.5.linux-amd64.tar.gz.checksum && tar -C /usr/local -xvf go1.18.5.linux-amd64.tar.gz

# define PATH
ENV PATH /usr/local/go/bin:$PATH

# install stellar-etl as 'stellar' user to avoid docker shim errors
USER stellar
WORKDIR /home/stellar/etl

COPY . .
RUN go install

# clear entrypoint from stellar-core image
ENTRYPOINT []

CMD ["stellar-etl"]
