#!/bin/bash

# Build Deployment Package for AWS Lambda

# Define package name
PACKAGE_NAME=deploy.zip

# Remove any previous deployment package
if [ -f "$PACKAGE_NAME" ]; then
  rm "$PACKAGE_NAME"
fi

# Create a temporary directory for the deployment package
DEPLOY_DIR="deployment_package"
if [ -d "$DEPLOY_DIR" ]; then
  rm -rf "$DEPLOY_DIR"
fi
mkdir "$DEPLOY_DIR"

# Copy Lambda handler and helper modules
cp lambda_handler.py "$DEPLOY_DIR/"
cp helpers.py "$DEPLOY_DIR/"

# Copy the src folder (recursively)
cp -r src "$DEPLOY_DIR/"

# Copy any additional necessary files (e.g., configuration files if needed)
# Uncomment the following lines if you need to include additional files
# cp -r config "$DEPLOY_DIR/"

# Copy the requirements.txt file
if [ -f requirements.txt ]; then
    cp requirements.txt "$DEPLOY_DIR/"
fi

# Change to the deployment package directory and create a zip file
cd "$DEPLOY_DIR"
zip -r ../$PACKAGE_NAME .

cd ..

echo "Deployment package created: $PACKAGE_NAME" 