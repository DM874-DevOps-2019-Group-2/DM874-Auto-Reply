FROM golang:1.13.5-alpine

RUN apk add git make

RUN adduser --disabled-password golang
USER golang
WORKDIR /home/golang/


# COPY dependencies.txt /dependencies.txt
COPY main.go main.go
COPY Makefile Makefile
COPY dependencies.txt dependencies.txt

RUN cat dependencies.txt | xargs -I @ go get @
RUN make

CMD ["./main"]