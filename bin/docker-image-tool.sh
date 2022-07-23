#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# This script builds and pushes docker images when run from a release of Spark
# with Kubernetes support.

function error {
  echo "$@" 1>&2
  exit 1
}

if [ -z "${CONN_HOME}" ]; then
  CONN_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

CTX_DIR="$CONN_HOME/target/tmp/docker"

SCALA_VERSION="2.12"

function is_dev_build {
  [ ! -f "$CONN_HOME/RELEASE" ]
}

function cleanup_ctx_dir {
  if is_dev_build; then
    rm -rf "$CTX_DIR"
  fi
}

trap cleanup_ctx_dir EXIT

function image_ref {
  local image="$1"
  local add_repo="${2:-1}"
  if [ $add_repo = 1 ] && [ -n "$REPO" ]; then
    image="$REPO/$image"
  fi
  if [ -n "$TAG" ]; then
    image="$image:$TAG"
  fi
  echo "$image"
}

function docker_push {
  local image_name="$1"
  if [ ! -z $(docker images -q "$(image_ref ${image_name})") ]; then
    docker push "$(image_ref ${image_name})"
    if [ $? -ne 0 ]; then
      error "Failed to push $image_name Docker image."
    fi
  else
    echo "$(image_ref ${image_name}) image not found. Skipping push for this image."
  fi
}

function resolve_file {
  local FILE=$1
  if [ -n "$FILE" ]; then
    local DIR=$(dirname $FILE)
    DIR=$(cd $DIR && pwd)
    FILE="${DIR}/$(basename $FILE)"
  fi
  echo $FILE
}

# Create a smaller build context for docker in dev builds to make the build faster. Docker
# uploads all of the current directory to the daemon, and it can get pretty big with dev
# builds that contain test log files and other artifacts.
#
# Three build contexts are created, one for each image: base, pyspark, and sparkr. For them
# to have the desired effect, the docker command needs to be executed inside the appropriate
# context directory.
#
# Note: docker does not support symlinks in the build context.
function create_dev_build_context {(
  set -e

  mkdir -p $CTX_DIR
  cp "bin/Dockerfile" $CTX_DIR

  cp -r "connector/target/scala-$SCALA_VERSION/jars" "$CTX_DIR/jars"
)}

function img_ctx_dir {
  if is_dev_build; then
    echo "$CTX_DIR/$1"
  else
    echo "$CONN_HOME"
  fi
}

function build {
  local BUILD_ARGS

  if is_dev_build; then
    create_dev_build_context || error "Failed to create docker build context."
  fi

  local BUILD_ARGS=(
    ${BUILD_PARAMS[@]}
    --build-arg
    base_img=$SPARK_IMAGE
  )

  local ARCHS=${ARCHS:-"--platform linux/amd64,linux/arm64"}

  local DOCKERFILE=$(img_ctx_dir Dockerfile)

  (cd $(img_ctx_dir .) && docker build $NOCACHEARG "${BUILD_ARGS[@]}" \
    -t $(image_ref spark-cassandra) \
    -f "$DOCKERFILE" .)
  if [ $? -ne 0 ]; then
    error "Failed to build Spark JVM Docker image, please refer to Docker build output for details."
  fi
  if [ "${CROSS_BUILD}" != "false" ]; then
  (cd $(img_ctx_dir .) && docker buildx build $ARCHS $NOCACHEARG "${BUILD_ARGS[@]}" --push \
    -t $(image_ref spark-cassandra) \
    -f "$DOCKERFILE" .)
  fi
}

function push {
  docker_push "spark-cassandra"
}

function usage {
  cat <<EOF
Usage: $0 [options] [command]
Builds or pushes the built-in Spark Docker image.

Commands:
  build       Build image. Requires a repository address to be provided if the image will be
              pushed to a different registry.
  push        Push a pre-built image to a registry. Requires a repository address to be provided.

Options:
  -r repo               Repository address.
  -s spark              Base spark image to use.
  -t tag                Tag to apply to the built image, or to identify the image to be pushed.
  -n                    Build docker image with --no-cache
  -X                    Use docker buildx to cross build. Automatically pushes.
                        See https://docs.docker.com/buildx/working-with-buildx/ for steps to setup buildx.
  -b arg                Build arg to build or push the image. For multiple build args, this option needs to
                        be used separately for each build arg.

Using minikube when building images will do so directly into minikube's Docker daemon.
There is no need to push the images into minikube in that case, they'll be automatically
available when running applications inside the minikube cluster.

Check the following documentation for more information on using the minikube Docker daemon:

  https://kubernetes.io/docs/getting-started-guides/minikube/#reusing-the-docker-daemon

Examples:
  - Build image in minikube with tag "testing"
    $0 -m -t testing build

  - Build PySpark docker image
    $0 -r docker.io/myrepo -t v2.3.0 -p kubernetes/dockerfiles/spark/bindings/python/Dockerfile build

  - Build and push image with tag "v2.3.0" to docker.io/myrepo
    $0 -r docker.io/myrepo -t v2.3.0 build
    $0 -r docker.io/myrepo -t v2.3.0 push

  - Build and push JDK11-based image with tag "v3.0.0" to docker.io/myrepo
    $0 -r docker.io/myrepo -t v3.0.0 -b java_image_tag=11-jre-slim build
    $0 -r docker.io/myrepo -t v3.0.0 push

  - Build and push JDK11-based image for multiple archs to docker.io/myrepo
    $0 -r docker.io/myrepo -t v3.0.0 -X -b java_image_tag=11-jre-slim build
    # Note: buildx, which does cross building, needs to do the push during build
    # So there is no separate push step with -X

EOF
}

if [[ "$@" = *--help ]] || [[ "$@" = *-h ]]; then
  usage
  exit 0
fi

REPO=
TAG=
SPARK_IMAGE=
NOCACHEARG=
BUILD_PARAMS=
CROSS_BUILD="false"
while getopts f:p:s:R:mr:t:Xnb:u: option
do
 case "${option}"
 in
 r) REPO=${OPTARG};;
 t) TAG=${OPTARG};;
 s) SPARK_IMAGE=${OPTARG};;
 n) NOCACHEARG="--no-cache";;
 b) BUILD_PARAMS=${BUILD_PARAMS}" --build-arg "${OPTARG};;
 X) CROSS_BUILD=1;;
 esac
done

case "${@: -1}" in
  build)
    build
    ;;
  push)
    if [ -z "$REPO" ]; then
      usage
      exit 1
    fi
    push
    ;;
  *)
    usage
    exit 1
    ;;
esac
