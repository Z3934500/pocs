# Environment-Aware Terraform

The original flat `deploy/terraform` folder was a compact PoC template and is not part of this repository. This folder shows the company-style split for Dev, Staging and Production.

```text
dev        -> cce-feature-platform-dev
staging    -> cce-feature-platform-staging
production -> cce-feature-platform-production
```

Each environment gets distinct AWS resource names and tags through `var.environment`.

## What Terraform Owns

```text
MSK cluster
ElastiCache Redis replication group
ElastiCache subnet group
environment tags and outputs
```

Terraform does not promote application images. Image promotion is owned by CI/CD and Argo CD or Helm.

## Environment Sizing

| Environment | MSK | Redis | Apply policy |
| --- | --- | --- | --- |
| Dev | 1 x `kafka.t3.small`, 100GB | `cache.t4g.micro`, no replica | `apply_immediately=true` |
| Staging | 2 x `kafka.t3.small`, 200GB | `cache.t4g.small`, 1 replica | `apply_immediately=true` |
| Production | 3 x `kafka.m5.large`, 500GB | `cache.m6g.large`, 1 replica | `apply_immediately=false` |

## Step By Step

Copy an example file and replace subnet/security group IDs:

```powershell
Copy-Item environments\dev.tfvars.example environments\dev.tfvars
Copy-Item environments\staging.tfvars.example environments\staging.tfvars
Copy-Item environments\production.tfvars.example environments\production.tfvars
```

Plan Dev:

```powershell
terraform init
terraform workspace new dev
terraform plan -var-file environments\dev.tfvars
```

Apply Dev:

```powershell
terraform apply -var-file environments\dev.tfvars
```

Plan Staging:

```powershell
terraform workspace new staging
terraform plan -var-file environments\staging.tfvars
```

Apply Staging:

```powershell
terraform apply -var-file environments\staging.tfvars
```

Plan Production:

```powershell
terraform workspace new production
terraform plan -var-file environments\production.tfvars
```

Apply Production after change approval:

```powershell
terraform apply -var-file environments\production.tfvars
```

## State Strategy

For a real company, prefer separate remote state keys or Terraform Cloud workspaces:

```text
cce-feature-platform/dev/terraform.tfstate
cce-feature-platform/staging/terraform.tfstate
cce-feature-platform/production/terraform.tfstate
```

This prevents a Dev change from accidentally planning against Production infrastructure.

## SDLC Mapping

| SDLC stage | Terraform action |
| --- | --- |
| Sprint build | Update tfvars or resource definition on branch |
| Pull request | Run `terraform fmt` and `terraform plan` for Dev/Staging |
| Release candidate | Run Staging plan and apply after approval |
| Production release | Run Production plan, review diff, apply in change window |
| Operate | Monitor MSK/Redis, adjust capacity through Terraform PR |

