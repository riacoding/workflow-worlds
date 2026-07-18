import { Stack, CfnOutput, RemovalPolicy, Duration, Aws } from 'aws-cdk-lib'
import type { StackProps } from 'aws-cdk-lib'
import { Table, AttributeType, BillingMode, ProjectionType } from 'aws-cdk-lib/aws-dynamodb'
import { Queue } from 'aws-cdk-lib/aws-sqs'
import { Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam'
import { CfnScheduleGroup } from 'aws-cdk-lib/aws-scheduler'
import type { Construct } from 'constructs'

export interface WorkflowAwsStackProps extends StackProps {
  /** Human-chosen project name. Namespaces every resource to avoid collisions with other projects in the same account/region. */
  projectName: string
  /** Deployment stage, e.g. dev / staging / prod. */
  stage: string
  /** DynamoDB table removal policy override. Defaults to RETAIN when stage === 'prod', DESTROY otherwise. */
  removalPolicy?: 'destroy' | 'retain'
}

export class WorkflowAwsStack extends Stack {
  constructor(scope: Construct, id: string, props: WorkflowAwsStackProps) {
    super(scope, id, props)

    const { projectName, stage } = props
    const namePrefix = `${projectName}-${stage}`
    const tableName = `${namePrefix}-workflow`
    const queueName = `${namePrefix}-workflow-queue`
    const schedulerGroupName = `${namePrefix}-workflow`
    const schedulerRoleName = `${namePrefix}-workflow-scheduler`

    const tableRemovalPolicy =
      props.removalPolicy === 'destroy'
        ? RemovalPolicy.DESTROY
        : props.removalPolicy === 'retain'
          ? RemovalPolicy.RETAIN
          : stage === 'prod'
            ? RemovalPolicy.RETAIN
            : RemovalPolicy.DESTROY

    // -------------------------------------------------------------------
    // DynamoDB — single-table design, mirrors packages/aws/src/aws.ts's
    // ensureTable(): PK/SK + GSI1 + GSI2 + TTL on `ttl`.
    // -------------------------------------------------------------------
    const table = new Table(this, 'Table', {
      tableName,
      partitionKey: { name: 'PK', type: AttributeType.STRING },
      sortKey: { name: 'SK', type: AttributeType.STRING },
      billingMode: BillingMode.PAY_PER_REQUEST,
      timeToLiveAttribute: 'ttl',
      removalPolicy: tableRemovalPolicy,
    })

    table.addGlobalSecondaryIndex({
      indexName: 'GSI1',
      partitionKey: { name: 'GSI1PK', type: AttributeType.STRING },
      sortKey: { name: 'GSI1SK', type: AttributeType.STRING },
      projectionType: ProjectionType.ALL,
    })

    table.addGlobalSecondaryIndex({
      indexName: 'GSI2',
      partitionKey: { name: 'GSI2PK', type: AttributeType.STRING },
      sortKey: { name: 'GSI2SK', type: AttributeType.STRING },
      projectionType: ProjectionType.ALL,
    })

    // -------------------------------------------------------------------
    // SQS — mirrors ensureQueue(): standard queue, 60s visibility timeout,
    // 14 day retention.
    // -------------------------------------------------------------------
    const queue = new Queue(this, 'Queue', {
      queueName,
      visibilityTimeout: Duration.seconds(60),
      retentionPeriod: Duration.seconds(1209600),
    })

    // -------------------------------------------------------------------
    // EventBridge Scheduler — mirrors ensureSchedulerGroup(), plus the IAM
    // role the Scheduler needs to deliver to SQS for delays beyond SQS's
    // 12-hour ceiling (packages/aws/src/queue.ts's CreateScheduleCommand
    // target shape: { Arn: queueArn, RoleArn: schedulerRoleArn }). This role
    // is not created by today's auto-provisioning code — it's a manual step
    // there; here it's provisioned so long-delay retries work out of the box.
    // -------------------------------------------------------------------
    const schedulerGroup = new CfnScheduleGroup(this, 'SchedulerGroup', {
      name: schedulerGroupName,
    })

    const schedulerRole = new Role(this, 'SchedulerRole', {
      roleName: schedulerRoleName,
      // NOTE: an additional ArnLike condition scoping aws:SourceArn to this
      // schedule group is the AWS-documented pattern and does work at actual
      // schedule-invocation time, but EventBridge Scheduler's CreateSchedule
      // pre-flight validation has been observed to reject it outright with
      // "The execution role you provide must allow AWS EventBridge Scheduler
      // to assume the role" — a false negative confirmed via CloudTrail
      // (the real sts:AssumeRole succeeds) that clears the moment the
      // condition is dropped. Scoped to SourceAccount only; the role's
      // permissions are still limited to sqs:SendMessage on this one queue.
      assumedBy: new ServicePrincipal('scheduler.amazonaws.com', {
        conditions: {
          StringEquals: { 'aws:SourceAccount': Aws.ACCOUNT_ID },
        },
      }),
    })
    schedulerRole.node.addDependency(schedulerGroup)

    queue.grantSendMessages(schedulerRole)

    // -------------------------------------------------------------------
    // Outputs — consumed by scripts/print-env.ts to produce the exact
    // WORKFLOW_* env vars packages/aws/src/aws.ts's resolveAwsConfig() reads.
    // -------------------------------------------------------------------
    new CfnOutput(this, 'TableName', { value: table.tableName })
    new CfnOutput(this, 'QueueUrl', { value: queue.queueUrl })
    new CfnOutput(this, 'QueueArn', { value: queue.queueArn })
    new CfnOutput(this, 'QueueName', { value: queue.queueName })
    new CfnOutput(this, 'SchedulerGroupName', { value: schedulerGroupName })
    new CfnOutput(this, 'SchedulerRoleArn', { value: schedulerRole.roleArn })
    new CfnOutput(this, 'Region', { value: Aws.REGION })
  }
}
