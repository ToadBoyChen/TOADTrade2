#!/bin/bash
set -e

APP_NAME="tt2"
IMAGE_NAME="tt2:latest"
CONTAINER_NAME="tt2_container"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

FORCE_REBUILD=false
if [[ "$1" == "--rebuild" ]]; then
    FORCE_REBUILD=true
fi

if ! systemctl is-active --quiet docker; then
    echo "Starting Docker service..."
    sudo systemctl start docker
fi

docker stop "${CONTAINER_NAME}" >/dev/null 2>&1 || true

if $FORCE_REBUILD || ! docker image inspect "${IMAGE_NAME}" >/dev/null 2>&1; then
    echo "Building Docker image: ${IMAGE_NAME}"
    docker build -t "${IMAGE_NAME}" "${PROJECT_ROOT}"
else
    echo "Using existing image: ${IMAGE_NAME}"
fi

echo "Running ${CONTAINER_NAME}..."
docker run -it --rm \
    --name "${CONTAINER_NAME}" \
    -v "${PROJECT_ROOT}:/app" \
    -w /app \
    "${IMAGE_NAME}" \
    bash
