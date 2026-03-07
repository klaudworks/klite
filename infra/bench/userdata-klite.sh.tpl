#!/bin/bash
set -ex

dnf install -y docker sysstat
systemctl enable docker
systemctl start docker
usermod -aG docker ec2-user

# Create data directory on root volume
mkdir -p /data/klite
chown ec2-user:ec2-user /data/klite

# ECR login helper — instances use IAM role credentials
aws ecr get-login-password --region ${region} | docker login --username AWS --password-stdin ${ecr_url}
