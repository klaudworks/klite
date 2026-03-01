output "msk_bootstrap_plaintext" {
  description = "MSK plaintext bootstrap servers"
  value       = var.enable_msk ? aws_msk_cluster.bench[0].bootstrap_brokers : "(MSK not enabled)"
}

output "klite_public_ip" {
  description = "Public IP of klite EC2 instance"
  value       = aws_instance.klite.public_ip
}

output "klite_private_ip" {
  description = "Private IP of klite EC2 instance (use this as bootstrap server from bench client)"
  value       = aws_instance.klite.private_ip
}

output "bench_public_ip" {
  description = "Public IP of benchmark client EC2 instance"
  value       = aws_instance.bench.public_ip
}

output "ssh_klite" {
  description = "SSH command for klite instance"
  value       = "ssh ec2-user@${aws_instance.klite.public_ip}"
}

output "ssh_bench" {
  description = "SSH command for bench client instance"
  value       = "ssh ec2-user@${aws_instance.bench.public_ip}"
}

output "ecr_klite" {
  description = "ECR repository URL for klite image"
  value       = aws_ecr_repository.klite.repository_url
}

output "ecr_klite_bench" {
  description = "ECR repository URL for klite-bench image"
  value       = aws_ecr_repository.klite_bench.repository_url
}

output "s3_bucket" {
  description = "S3 bucket for klite data"
  value       = aws_s3_bucket.klite.id
}
