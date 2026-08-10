# Terraform / infrastructure-live boundary

This application repository contains the service code, Dockerfiles and Helm chart. Production AWS infrastructure is intentionally managed in a separate `infrastructure-live` repository so that cloud account state, KMS policies, network rules and environment approvals do not live beside application code.

The infrastructure repository should expose these environment outputs to the deployment pipeline:

- EKS cluster name and private API endpoint
- ECR repository URIs for `order-service`, `inventory-service` and `payment-service`
- Aurora PostgreSQL connection secret references
- MSK or SQS/SNS endpoints
- IAM role ARNs for GitHub OIDC and Kubernetes service accounts
- AMP workspace, CloudWatch log group and ADOT collector endpoints

Recommended layout:

```text
infrastructure-live/
  modules/{vpc,eks,ecr,aurora,msk,redis,observability,iam}/
  environments/{dev,staging,prod}/
```

Use an encrypted remote state backend, reviewed `terraform plan`, separate AWS accounts for production, and no plaintext secrets in variables or state. The application deployment sequence is documented in [`../../docs/PRODUCTION_DEPLOYMENT_DEVSECOPS.md`](../../docs/PRODUCTION_DEPLOYMENT_DEVSECOPS.md).


## Scheduled RDS/Aurora business-hours operation

The application repository does not create or mutate production database
infrastructure. The separate `infrastructure-live` repository should install
and operate AWS Instance Scheduler, then tag the approved RDS/Aurora instance:

```text
Schedule         = oms-weekday-business-hours
ScheduleTimezone = Asia/Shanghai
Environment      = staging|prod
Application      = inventory-oms
```

The schedule contract used by the application PoC is:

```text
MON-FRI, start 08:00, stop 20:30, Asia/Shanghai
```

Before enabling it, infrastructure-live must verify the application runbook
[`docs/SCHEDULED_SCALING_AND_RDS.md`](../../docs/SCHEDULED_SCALING_AND_RDS.md),
keep automatic backups/deletion protection according to the environment policy,
and expose the database endpoint/secret only after the startup health gate passes.

The Helm overlay
[`values-scheduled-scaling.yaml`](../helm/oms-services/values-scheduled-scaling.yaml)
is a configuration contract. It does not grant IAM permissions or stop a
database by itself. Database lifecycle permissions belong to the scheduler's
least-privilege infrastructure role, not to the application Pod role.
