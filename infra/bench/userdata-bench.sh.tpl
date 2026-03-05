#!/bin/bash
set -ex

dnf install -y docker
systemctl enable docker
systemctl start docker
usermod -aG docker ec2-user

# ECR login helper
aws ecr get-login-password --region ${region} | docker login --username AWS --password-stdin ${ecr_url_bench}
