FROM golang:1.13.5-alpine as goBuild

RUN mkdir /build/
WORKDIR /build
COPY go/* ./
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags "-s" -a -installsuffix cgo -o main

FROM alpine:latest
COPY --from=goBuild /build/main ./main


CMD ["./main"]
