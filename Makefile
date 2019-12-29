.PHONY: test

all:
	docker build -t auto-reply .

clean:
	main