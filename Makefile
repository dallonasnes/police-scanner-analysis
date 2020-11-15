build:
	time docker build -t app .

run:
	docker run -it --rm --name app app