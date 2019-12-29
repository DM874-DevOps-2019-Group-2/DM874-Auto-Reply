FROM golang:1.13.5-alpine as goBuild
ENV GO111MODULE=on
RUN mkdir /build
WORKDIR /build
COPY go/ ./
RUN go get
RUN go mod tidy
WORKDIR /build/messaging
RUN go test
WORKDIR /build
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags "-s" -a -installsuffix cgo -o main

FROM scratch
COPY --from=goBuild /build/main ./main

CMD ["./main"]
