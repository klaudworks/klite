region          = "eu-west-1"
aws_profile     = "klite-bench"

enable_msk        = false
msk_instance_type = "kafka.m7g.large"
msk_broker_count  = 3

klite_instance_type = "m8g.2xlarge"
bench_instance_type = "m8g.xlarge"

# EBS gp3 volume for klite (WAL storage).
# Baseline free: 3000 IOPS, 125 MiB/s. Max gp3: 16000 IOPS, 1000 MiB/s.
#
# Instance EBS baselines (sustained, not burst) — m7g and m8g are identical:
#   *.xlarge  = 156 MB/s (1250 Mbps)     *.2xlarge = 312 MB/s (2500 Mbps)
#   *.4xlarge = 625 MB/s (5000 Mbps)     *.8xlarge = 1250 MB/s (10 Gbps)
#
# Instance network baselines (for S3 upload):
#   *.xlarge  = 234 MB/s (1.875 Gbps)    *.2xlarge = 469 MB/s (3.75 Gbps)
#   *.4xlarge = 938 MB/s (7.5 Gbps)      *.8xlarge = 1875 MB/s (15 Gbps)
klite_ebs_size_gb    = 100
klite_ebs_iops       = 6000
klite_ebs_throughput = 300
