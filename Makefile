IMAGE_NAME := astronomerio/clickstream-loader:latest

build:
	docker build -t ${IMAGE_NAME} .

push:
	docker push ${IMAGE_NAME}

run:
	docker run -it ${IMAGE_NAME} sh
