FROM golang:1.14
ENV GOBIN /go/bin
RUN go get github.com/stellar/stellar-etl
WORKDIR /etl
COPY stellar-core.cfg .
RUN apt-get -qq update
RUN wget -qO - https://apt.stellar.org/SDF.asc | apt-key add -
RUN echo "deb https://apt.stellar.org xenial stable" | tee -a /etc/apt/sources.list.d/SDF.list

#Needed for stellar-core dependencies
RUN echo "deb http://deb.debian.org/debian buster-backports main" | tee -a /etc/apt/sources.list.d/buster-backports.list

RUN apt-get -qq update && apt-get -qq install -y stellar-core
CMD ["stellar-etl"]