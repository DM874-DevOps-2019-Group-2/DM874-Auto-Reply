.PHONY: test

main: go/main.go
	go run go/main.go

clean:
	main