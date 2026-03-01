###############################################################################
# MSK cluster — only created when enable_msk = true
###############################################################################

resource "aws_security_group" "msk" {
  count       = var.enable_msk ? 1 : 0
  name_prefix = "${local.name}-msk-"
  vpc_id      = data.aws_vpc.default.id
  description = "MSK brokers"

  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [data.aws_vpc.default.cidr_block]
    description = "Kafka plaintext"
  }

  ingress {
    from_port   = 9094
    to_port     = 9094
    protocol    = "tcp"
    cidr_blocks = [data.aws_vpc.default.cidr_block]
    description = "Kafka TLS"
  }

  ingress {
    from_port   = 2181
    to_port     = 2181
    protocol    = "tcp"
    cidr_blocks = [data.aws_vpc.default.cidr_block]
    description = "Zookeeper"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${local.name}-msk" }

  lifecycle { create_before_destroy = true }
}

resource "aws_msk_configuration" "bench" {
  count             = var.enable_msk ? 1 : 0
  name              = "${local.name}-config"
  kafka_versions    = ["3.7.x.kraft"]
  server_properties = <<-EOT
    auto.create.topics.enable=true
    default.replication.factor=3
    min.insync.replicas=2
    num.partitions=6
    log.retention.hours=1
  EOT
}

resource "aws_msk_cluster" "bench" {
  count                  = var.enable_msk ? 1 : 0
  cluster_name           = local.name
  kafka_version          = "3.7.x.kraft"
  number_of_broker_nodes = var.msk_broker_count

  broker_node_group_info {
    instance_type   = var.msk_instance_type
    client_subnets  = slice(data.aws_subnets.default.ids, 0, 3)
    security_groups = [aws_security_group.msk[0].id]

    storage_info {
      ebs_storage_info {
        volume_size = 100
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.bench[0].arn
    revision = aws_msk_configuration.bench[0].latest_revision
  }

  client_authentication {
    unauthenticated = true
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "PLAINTEXT"
      in_cluster    = false
    }
  }

  tags = { Name = local.name }
}
