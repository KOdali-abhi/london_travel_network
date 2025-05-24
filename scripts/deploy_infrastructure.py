from aws_cdk import (
    core,
    aws_s3 as s3,
    aws_emr as emr,
    aws_glue as glue,
    aws_lambda as lambda_,
    aws_kinesis as kinesis,
    aws_sns as sns,
    aws_iam as iam,
    aws_athena as athena,
    aws_quicksight as quicksight
)

class TflDataPipelineStack(core.Stack):
    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # S3 Buckets for different data zones
        raw_bucket = s3.Bucket(
            self, "RawDataBucket",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED
        )

        processed_bucket = s3.Bucket(
            self, "ProcessedDataBucket",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED
        )

        # Kinesis Data Stream for real-time ingestion
        stream = kinesis.Stream(
            self, "TflDataStream",
            stream_name="tfl-data-stream",
            retention_period=core.Duration.hours(24),
            shard_count=4
        )

        # EMR Cluster
        emr_role = iam.Role(
            self, "EMRServiceRole",
            assumed_by=iam.ServicePrincipal("elasticmapreduce.amazonaws.com")
        )

        cluster = emr.CfnCluster(
            self, "TflDataProcessingCluster",
            instances=emr.CfnCluster.JobFlowInstancesConfigProperty(
                core_instance_group=emr.CfnCluster.InstanceGroupConfigProperty(
                    instance_count=4,
                    instance_type="r5.2xlarge",
                    market="ON_DEMAND"
                ),
                master_instance_group=emr.CfnCluster.InstanceGroupConfigProperty(
                    instance_count=1,
                    instance_type="r5.2xlarge",
                    market="ON_DEMAND"
                )
            ),
            name="tfl-data-processing-cluster",
            service_role=emr_role.role_name,
            applications=[
                emr.CfnCluster.ApplicationProperty(name="Spark"),
                emr.CfnCluster.ApplicationProperty(name="Hive"),
                emr.CfnCluster.ApplicationProperty(name="Hadoop")
            ],
            release_label="emr-6.5.0"
        )

        # Glue Data Catalog Database
        glue_database = glue.CfnDatabase(
            self, "TflGlueDatabase",
            catalog_id=core.Aws.ACCOUNT_ID,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="tfl_data_catalog"
            )
        )

        # Lambda for data ingestion
        ingestion_lambda = lambda_.Function(
            self, "TflDataIngestionLambda",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="index.handler",
            code=lambda_.Code.from_asset("lambda/ingestion"),
            timeout=core.Duration.minutes(5),
            memory_size=1024,
            environment={
                "KINESIS_STREAM": stream.stream_name,
                "RAW_BUCKET": raw_bucket.bucket_name
            }
        )

        # SNS Topic for alerts
        alerts_topic = sns.Topic(
            self, "DataQualityAlertsTopic",
            topic_name="tfl-data-quality-alerts"
        )

        # Athena Workgroup
        athena_workgroup = athena.CfnWorkGroup(
            self, "TflAthenaWorkgroup",
            name="tfl-analytics",
            recursive_delete_option=True,
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{processed_bucket.bucket_name}/athena-results/"
                )
            )
        )

        # Grant permissions
        raw_bucket.grant_read_write(ingestion_lambda)
        stream.grant_write(ingestion_lambda)
        processed_bucket.grant_read_write(cluster.attr_master_public_dns)
        alerts_topic.grant_publish(cluster.attr_master_public_dns)

        # Output important resource information
        core.CfnOutput(
            self, "RawBucketName",
            value=raw_bucket.bucket_name,
            description="Name of the raw data bucket"
        )
        core.CfnOutput(
            self, "ProcessedBucketName",
            value=processed_bucket.bucket_name,
            description="Name of the processed data bucket"
        )
        core.CfnOutput(
            self, "KinesisStreamName",
            value=stream.stream_name,
            description="Name of the Kinesis data stream"
        )
        core.CfnOutput(
            self, "AlertsTopicArn",
            value=alerts_topic.topic_arn,
            description="ARN of the SNS alerts topic"
        )

app = core.App()
TflDataPipelineStack(app, "TflDataPipeline")
app.synth() 