.PHONY: test

test: main
	termite -e "bash -c \"source set_test_environment.sh; ./main; read line\""

main: main.go
	go build main.go

clean:
	main