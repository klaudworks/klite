###############################################################################
# Default VPC + subnets
###############################################################################

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

data "aws_ssm_parameter" "al2023_ami" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
}

locals {
  name = "klite-bench"
}

###############################################################################
# SSH key pair
###############################################################################

resource "aws_key_pair" "bench" {
  key_name   = "${local.name}-key"
  public_key = var.ssh_public_key
}

###############################################################################
# Security group
###############################################################################

resource "aws_security_group" "ec2" {
  name_prefix = "${local.name}-ec2-"
  vpc_id      = data.aws_vpc.default.id
  description = "EC2 instances for klite and bench"

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "SSH"
  }

  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [data.aws_vpc.default.cidr_block]
    description = "Kafka (klite)"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${local.name}-ec2" }

  lifecycle { create_before_destroy = true }
}

###############################################################################
# ECR repositories
###############################################################################

resource "aws_ecr_repository" "klite" {
  name                 = "klite"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
}

resource "aws_ecr_repository" "klite_bench" {
  name                 = "klite-bench"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
}

###############################################################################
# S3 bucket for klite data
###############################################################################

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "klite" {
  bucket        = "${local.name}-data-${random_id.bucket_suffix.hex}"
  force_destroy = true

  tags = { Name = "${local.name}-data" }
}

###############################################################################
# IAM role for EC2 (ECR pull + SSM + S3)
###############################################################################

resource "aws_iam_role" "ec2" {
  name = "${local.name}-ec2"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ec2_ssm" {
  role       = aws_iam_role.ec2.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy_attachment" "ec2_ecr" {
  role       = aws_iam_role.ec2.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

resource "aws_iam_role_policy" "ec2_s3" {
  name = "${local.name}-s3"
  role = aws_iam_role.ec2.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
      Resource = [
        aws_s3_bucket.klite.arn,
        "${aws_s3_bucket.klite.arn}/*",
      ]
    }]
  })
}

resource "aws_iam_instance_profile" "ec2" {
  name = "${local.name}-ec2"
  role = aws_iam_role.ec2.name
}

###############################################################################
# EC2: klite broker
###############################################################################

resource "aws_instance" "klite" {
  ami                    = data.aws_ssm_parameter.al2023_ami.value
  instance_type          = var.klite_instance_type
  key_name               = aws_key_pair.bench.key_name
  subnet_id              = data.aws_subnets.default.ids[0]
  vpc_security_group_ids = [aws_security_group.ec2.id]
  iam_instance_profile   = aws_iam_instance_profile.ec2.name

  root_block_device {
    volume_type = "gp3"
    volume_size = 100
    iops        = 3000
    throughput  = 125
  }

  user_data = base64encode(templatefile("${path.module}/userdata-klite.sh.tpl", {
    region  = var.region
    ecr_url = aws_ecr_repository.klite.repository_url
  }))

  tags = { Name = "${local.name}-klite" }
}

###############################################################################
# EC2: benchmark client
###############################################################################

resource "aws_instance" "bench" {
  ami                    = data.aws_ssm_parameter.al2023_ami.value
  instance_type          = var.bench_instance_type
  key_name               = aws_key_pair.bench.key_name
  subnet_id              = data.aws_subnets.default.ids[0]
  vpc_security_group_ids = [aws_security_group.ec2.id]
  iam_instance_profile   = aws_iam_instance_profile.ec2.name

  root_block_device {
    volume_type = "gp3"
    volume_size = 20
  }

  user_data = base64encode(templatefile("${path.module}/userdata-bench.sh.tpl", {
    region        = var.region
    ecr_url_bench = aws_ecr_repository.klite_bench.repository_url
  }))

  tags = { Name = "${local.name}-bench" }
}
