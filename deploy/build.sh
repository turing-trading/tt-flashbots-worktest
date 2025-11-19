#!/bin/bash
set -e

# Build and push script for Flashbots live streaming Docker image
# Usage: ./deploy/build.sh [tag]

TAG="${1:-latest}"
IMAGE_NAME="docker.turing-trading.org/flashbots/mev-boost"
FULL_IMAGE="${IMAGE_NAME}:${TAG}"

echo "Building Docker image: ${FULL_IMAGE}"

# Login to Docker registry if credentials are available
if [ -f "deploy/dockerconfig.json" ]; then
    echo "Logging in to docker.turing-trading.org..."
    docker login docker.turing-trading.org -u admin -p "9yAsA1k8n#B@IfzE6N8f"
fi

# Build the image
docker build \
    -f deploy/Dockerfile \
    -t "${FULL_IMAGE}" \
    .

echo "Image built successfully: ${FULL_IMAGE}"

# Tag as latest if a version tag was provided
if [ "$TAG" != "latest" ]; then
    docker tag "${FULL_IMAGE}" "${IMAGE_NAME}:latest"
    echo "Also tagged as: ${IMAGE_NAME}:latest"
fi

# Push to registry
echo "Pushing image to registry..."
docker push "${FULL_IMAGE}"

if [ "$TAG" != "latest" ]; then
    docker push "${IMAGE_NAME}:latest"
fi

echo "✅ Image pushed successfully!"
echo ""
echo "Image: ${FULL_IMAGE}"
echo ""
echo "Deploy to Kubernetes:"
echo "  kubectl apply -f deploy/k8s/"
echo ""
echo "Or update just the deployment:"
echo "  kubectl rollout restart deployment/flashbots-live -n flashbots"
