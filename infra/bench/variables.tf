variable "region" {
  description = "AWS region"
  type        = string
  default     = "eu-west-1"
}

variable "aws_profile" {
  description = "AWS CLI profile name"
  type        = string
  default     = "klite-bench"
}

variable "ssh_public_key" {
  description = "SSH public key for EC2 access (paste the contents of ~/.ssh/id_ed25519.pub or similar)"
  type        = string
}

variable "enable_msk" {
  description = "Whether to create the MSK cluster (takes ~20min, costs money)"
  type        = bool
  default     = false
}

variable "msk_instance_type" {
  description = "MSK broker instance type"
  type        = string
  default     = "kafka.m7g.large"
}

variable "msk_broker_count" {
  description = "Number of MSK brokers (must be multiple of AZ count)"
  type        = number
  default     = 3
}

variable "klite_instance_type" {
  description = "EC2 instance type for klite (Graviton arm64)"
  type        = string
  default     = "m7g.xlarge"
}

variable "bench_instance_type" {
  description = "EC2 instance type for benchmark client (Graviton arm64)"
  type        = string
  default     = "m7g.large"
}
