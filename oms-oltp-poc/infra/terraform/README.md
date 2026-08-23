# OMS OLTP — Infrastructure Blueprint (Terraform)

This directory contains the **production VPC blueprint** for `oms-oltp-poc`.  
It is **not deployed** — the local PoC uses Docker Compose + SQLite.  
Use this as an IaC reference that maps directly to `SAA_SECURITY_MAPPING.md`.

## Architecture

```
Internet
    │
[Internet Gateway]
    │
┌───────────────────────── VPC 10.0.0.0/16 ──────────────────────────┐
│                                                                      │
│  ┌── AZ-A (ap-southeast-1a) ──┐  ┌── AZ-B (ap-southeast-1b) ──┐   │
│  │                             │  │                              │   │
│  │  Public 10.0.1.0/24         │  │  Public 10.0.2.0/24         │   │
│  │  [ALB]  [NAT GW ← EIP]      │  │  [ALB cross-AZ]             │   │
│  │                             │  │                              │   │
│  │  Private App 10.0.11.0/24   │  │  Private App 10.0.12.0/24   │   │
│  │  [EKS Node: Order/Inv/Pay]  │  │  [EKS Node: replicas]       │   │
│  │                             │  │                              │   │
│  │  Private Data 10.0.21.0/24  │  │  Private Data 10.0.22.0/24  │   │
│  │  [Aurora Primary]           │  │  [Aurora Replica]            │   │
│  │  [Redis Primary]            │  │  [Redis Replica]             │   │
│  │  [MSK Broker-1]             │  │  [MSK Broker-2]              │   │
│  └─────────────────────────────┘  └──────────────────────────────┘  │
│                                                                      │
│  VPC Endpoints (private DNS): S3 · ECR · CloudWatch Logs ·          │
│                                Secrets Manager                       │
└──────────────────────────────────────────────────────────────────────┘
```

## Security Group Rules Summary

| SG | Inbound | Outbound |
|---|---|---|
| `sg-alb` | 443 from 0.0.0.0/0 | 8081-8083 to private-app CIDRs |
| `sg-app` | 8081-8083 from sg-alb; all from self | all (NAT GW handles egress) |
| `sg-aurora` | 5432 from sg-app | — |
| `sg-redis` | 6379 from sg-app | — |
| `sg-msk` | 9094 from sg-app; 9094 from self | — |
| `sg-vpce` | 443 from private-app CIDRs | — |

## Subnet Design Rationale

- **Public subnets**: only ALB and NAT GW live here. EKS nodes and databases have no public IPs.
- **Private App subnets**: EKS nodes route outbound via NAT GW. Inbound only from ALB.
- **Private Data subnets**: Aurora, Redis, MSK have no internet route at all — not even via NAT GW. They can only be reached by `sg-app`.
- **VPC Endpoints**: AWS service calls (ECR image pull, CloudWatch log push, Secrets Manager reads) never leave the AWS network. Eliminates the need for NAT bandwidth charges on AWS API traffic.

## Local PoC Mapping

| This Terraform | Local PoC equivalent |
|---|---|
| Aurora Multi-AZ | SQLite file |
| MSK Kafka | `messaging.transport=logging` (stdout) |
| Redis Cluster | In-process `ConcurrentHashMap` (seckill disabled) |
| ALB + WAF | `uvicorn` on `localhost:8000` |
| EKS pods | `docker-compose up` |
| NAT GW / IGW | Docker bridge network port mapping |

## Usage (when ready to deploy)

```bash
cd infra/terraform
terraform init
terraform plan -var="environment=staging"
terraform apply -var="environment=staging"
```

> **Do not** run `terraform apply` against a production account without:
> - Enabling S3 remote state backend (see `main.tf` comments)
> - Configuring Checkov / tfsec in CI
> - Reviewing cost: two NAT GWs ≈ $65/month base before data transfer
