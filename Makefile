DATE := $(shell date +"%Y%m%d_%H%M%S")
TAG := $(shell git describe --tags --always)
PREFIX := $(shell git config --get remote.origin.url | tr ':.' '/'  | rev | cut -d '/' -f 3 | rev)
REPO_NAME := $(shell git config --get remote.origin.url | tr ':.' '/'  | rev | cut -d '/' -f 2 | rev)

all: push

install:
	pip3 install -r requirements.txt

test:
	bats test.sh

image: install
	docker build -t $(PREFIX)/$(REPO_NAME):$(DATE) . # Build new image and automatically tag it as latest
	docker tag $(PREFIX)/$(REPO_NAME):$(DATE) $(PREFIX)/$(REPO_NAME):$(TAG)

push: image
	docker push $(PREFIX)/$(REPO_NAME):$(DATE) # Push image tagged as latest to repository
	
release: image
	docker tag $(PREFIX)/$(REPO_NAME):$(DATE) $(PREFIX)/$(REPO_NAME):latest  # Tag image as latest
	docker push $(PREFIX)/$(REPO_NAME):latest # Push image tagged as latest to repository
	docker tag $(PREFIX)/$(REPO_NAME):$(DATE) $(PREFIX)/$(REPO_NAME):$(TAG)  # Add the version tag to the latest image
	docker push $(PREFIX)/$(REPO_NAME):$(TAG) # Push version tagged image to repository (since this image is already pushed it will simply create or update version tag)

clean: