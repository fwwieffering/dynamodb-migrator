run:
	go install
	dynamodb-migrate

container:
	CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main .
	docker build -t dynamodb-migrator:latest .

# this is only for me to use
publish: container
	$(eval sha := $(shell git rev-parse HEAD))
	docker tag dynamodb-migrator:latest fwieffering/dynamodb-migrator:latest
	docker push fwieffering/dynamodb-migrator:latest
	docker tag dynamodb-migrator:latest fwieffering/dynamodb-migrator:$(sha)
	docker push fwieffering/dynamodb-migrator:$(sha)
