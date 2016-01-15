FROM golang
ENV GO15VENDOREXPERIMENT=1
ADD . /go/src/github.com/travisjeffery/appuser-transform
RUN go install github.com/travisjeffery/appuser-transform
ENTRYPOINT ["/go/bin/appuser-transform"]
