#!/bin/bash

if [ -z "$KUBERNETES_TOKEN" ]
then
	>&2 echo "KUBERNETES_TOKEN not set"
	exit 22
fi

if [ -z "$TRAVIS_COMMIT" ]
then
	>&2 echo "TRAVIS_COMMIT not set"
	exit 22
fi


docker run \
	--env KUBERNETES_TOKEN \
	--env DOCKER_IMAGE_SLUG=dm874/auto-reply \
	--env DOCKER_IMAGE_TAG=$TRAVIS_COMMIT \
	--env SERVICE=auto-reply \
	--env CONTAINER=auto-reply-container \
	dm874/deploy 
