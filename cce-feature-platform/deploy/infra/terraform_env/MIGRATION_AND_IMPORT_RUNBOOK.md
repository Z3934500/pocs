# Migration And Import Runbook

Use this runbook when moving existing MSK and Redis resources into `deploy/infra/terraform_env`.

## 1. Decide Migration Type

| Migration type | Use when |
| --- | --- |
| New Terraform apply | Environment does not exist yet |
| `terraform import` | Resources exist but are not in Terraform state |
| `terraform state mv` | Resources are already in a Terraform state and only addresses changed |
| `terraform init -migrate-state` | Backend location is changing |

## 2. Import Existing Dev Resources

```bash
cd deploy/infra/terraform_env
terraform init
terraform workspace new dev || terraform workspace select dev

terraform import aws_msk_cluster.this <dev-msk-cluster-arn>
terraform import aws_elasticache_subnet_group.this cce-feature-platform-dev
terraform import aws_elasticache_replication_group.redis cce-feature-platform-dev

terraform plan -var-file environments/dev.tfvars
```

## 3. Import Existing Staging Resources

```bash
cd deploy/infra/terraform_env
terraform init
terraform workspace new staging || terraform workspace select staging

terraform import aws_msk_cluster.this <staging-msk-cluster-arn>
terraform import aws_elasticache_subnet_group.this cce-feature-platform-staging
terraform import aws_elasticache_replication_group.redis cce-feature-platform-staging

terraform plan -var-file environments/staging.tfvars
```

## 4. Import Existing Production Resources

Production import should happen during a controlled change window.

```bash
cd deploy/infra/terraform_env
terraform init
terraform workspace new production || terraform workspace select production

terraform import aws_msk_cluster.this <production-msk-cluster-arn>
terraform import aws_elasticache_subnet_group.this cce-feature-platform-production
terraform import aws_elasticache_replication_group.redis cce-feature-platform-production

terraform plan -var-file environments/production.tfvars
```

Do not run `terraform apply` until the plan is reviewed and no unexpected replacement is shown.

## 5. If Moving From Old Terraform State

Back up old state first, from wherever the pre-split PoC template is checked out
— that flat `deploy/terraform` folder is not part of this repository, so there is
no path here to `cd` into:

```bash
cd <old-poc-checkout>/deploy/terraform
terraform state pull > old-terraform-state.backup.json
```

Then prefer import into the new environment state unless the resource addresses are simple enough for `terraform state mv`.

Example state move pattern:

```bash
terraform state mv aws_msk_cluster.this aws_msk_cluster.this
terraform state mv aws_elasticache_subnet_group.this aws_elasticache_subnet_group.this
terraform state mv aws_elasticache_replication_group.redis aws_elasticache_replication_group.redis
```

Only use `state mv` when source and destination state handling is fully understood. For most team migrations, import is easier to audit.

## 6. Validation After Migration

```bash
terraform fmt -check -recursive
terraform validate
terraform plan -var-file environments/<env>.tfvars
```

Expected result:

```text
No unexpected destroy
No unexpected replacement
Tags match environment
MSK broker count and size match target environment
Redis node type and replica count match target environment
```

## 7. Rollback

Terraform import itself does not change cloud resources. If import mapping is wrong:

```bash
terraform state rm aws_msk_cluster.this
terraform state rm aws_elasticache_subnet_group.this
terraform state rm aws_elasticache_replication_group.redis
```

Then re-import the correct resource IDs.

If an apply has already changed resources, rollback depends on the specific change. Use the previous tfvars, previous Git commit or previous state backup, then run a reviewed plan.

