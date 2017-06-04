FROM golang:1.8-alpine

ENV WORK_DIR /go/src/github.com/JetMuffin/google-cluster-simulator

WORKDIR $WORK_DIR
COPY . $WORK_DIR

RUN go build

