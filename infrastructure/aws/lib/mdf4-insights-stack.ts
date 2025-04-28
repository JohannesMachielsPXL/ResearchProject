import * as cdk from 'aws-cdk-lib';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';
import * as logs from 'aws-cdk-lib/aws-logs';
import {Construct} from 'constructs';
import {BaseStack, Context} from "./base";
import {createStackOutputParameter, getGlobalName} from "./common";


export interface Mdf4InsightsStackProps extends cdk.StackProps {

}


export class Mdf4InsightsStack extends BaseStack {
    readonly appRole: iam.IRole;

    constructor(scope: Construct, ctx: Context, id: string, props: Mdf4InsightsStackProps, tags?: {
        [key: string]: string
    }) {
        super(scope, ctx, id, {...props, analyticsReporting: false}, tags);

        // Create role for the app, to register permissions
        this.appRole = new iam.Role(this, "LambdaRole", {
            roleName: getGlobalName(ctx, 'lambda-role'),
            assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
            inlinePolicies: {
                "lambda-executor": new iam.PolicyDocument({
                    assignSids: true,
                    statements: [
                        new iam.PolicyStatement({
                            effect: iam.Effect.ALLOW,
                            actions: ["ec2:DescribeTags",
                                "cloudwatch:GetMetricStatistics",
                                "cloudwatch:ListMetrics",
                                "logs:CreateLogGroup",
                                "logs:CreateLogStream",
                                "logs:PutLogEvents",
                                "logs:DescribeLogStreams"],
                            resources: ["*"]
                        }),
                        new iam.PolicyStatement({
                            effect: iam.Effect.ALLOW,
                            actions: ["lambda:InvokeFunction"],
                            resources: ["*"]
                        }),
                        new iam.PolicyStatement({
                            effect: iam.Effect.ALLOW,
                            actions: [
                                "ec2:CreateNetworkInterface",
                                "ec2:DescribeNetworkInterfaces",
                                "ec2:DeleteNetworkInterface"
                            ],
                            resources: ["*"]
                        })
                    ]
                })
            }
        });
        createStackOutputParameter(this, ctx, {
            id: 'AppRoleArn',
            key: 'app-role-arn',
            value: this.appRole.roleArn,
            stackName: id,
            description: 'App Role Arn',
            cfnOutput: true
        });

        // Deploy the zip with mdf2parquet
        const mdf2parquet = new lambda.Function(this, 'mdf2parquet-fnc', {
            functionName: getGlobalName(ctx, 'mdf2parquet-fnc'),
            runtime: lambda.Runtime.PYTHON_3_11,
            handler: "lambda_function.lambda_handler",
            code: lambda.Code.fromAsset(`${__dirname}/contrib/mdf2parquet/mdf-to-parquet-lambda-function-v2.0.7.zip`),
            role: this.appRole,
            architecture: lambda.Architecture.X86_64,
            memorySize: 256,
            logRetention: logs.RetentionDays.FIVE_DAYS
        });
        createStackOutputParameter(this, ctx, {
            id: 'Mdf2ParquetFncArn',
            key: 'mdf2parquet-fnc-arn',
            value: mdf2parquet.functionArn,
            stackName: id,
            description: 'MDF2Parquet Function Arn',
            cfnOutput: true
        });

        // Define input bucket
        const mdfInputBucket = new s3.Bucket(this, 'mdf-input', {
            bucketName: getGlobalName(ctx, 'raw-data'),
            publicReadAccess: false,
            blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
            accessControl: s3.BucketAccessControl.PRIVATE,
            objectOwnership: s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
            removalPolicy: cdk.RemovalPolicy.DESTROY,
            autoDeleteObjects: true,
            versioned: false
        });
        mdfInputBucket.grantRead(this.appRole);
        createStackOutputParameter(this, ctx, {
            id: 'MdfInputBucketArn',
            key: 'mdf-input-bucket-arn',
            value: mdfInputBucket.bucketArn,
            stackName: id,
            description: 'MDF Input Bucket Arn',
            cfnOutput: true
        });
        createStackOutputParameter(this, ctx, {
            id: 'MdfInputBucketName',
            key: 'mdf-input-bucket-name',
            value: mdfInputBucket.bucketName,
            stackName: id,
            description: 'MDF Input Bucket Name',
            cfnOutput: true
        });

        // Upload the DBC file to the input bucket
        new s3deploy.BucketDeployment(this, 'dbc-file-deployment', {
            destinationBucket: mdfInputBucket,
            sources: [s3deploy.Source.asset(`${__dirname}/../../shared/`)],
            destinationKeyPrefix: 'shared/',
        });

        // Allow the input bucket to invoke the Lambda
        mdf2parquet.addPermission('allow-invoke-from-input-bucket', {
            action: 'lambda:InvokeFunction',
            principal: new iam.ServicePrincipal('s3.amazonaws.com'),
            sourceArn: mdfInputBucket.bucketArn,
            sourceAccount: ctx.deployment.account
        });

        // Set up S3 event notifications
        for (const suffix of ['.MF4', '.MFC', '.MFE', '.MFM']) {
            mdfInputBucket.addEventNotification(
                s3.EventType.OBJECT_CREATED,
                new s3n.LambdaDestination(mdf2parquet),
                {
                    suffix: suffix
                }
            );
        }

        // Define output bucket
        const parquetOutputBucket = new s3.Bucket(this, 'parquet-output', {
            bucketName: `${mdfInputBucket.bucketName}-parquet`,
            publicReadAccess: false,
            blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
            accessControl: s3.BucketAccessControl.PRIVATE,
            objectOwnership: s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
            removalPolicy: cdk.RemovalPolicy.DESTROY,
            autoDeleteObjects: true,
            versioned: false
        });
        parquetOutputBucket.grantReadWrite(this.appRole);
        createStackOutputParameter(this, ctx, {
            id: 'ParquetOutputBucketArn',
            key: 'parquet-output-bucket-arn',
            value: parquetOutputBucket.bucketArn,
            stackName: id,
            description: 'Parquet Input Bucket Arn',
            cfnOutput: true
        });
        createStackOutputParameter(this, ctx, {
            id: 'ParquetOutputBucketName',
            key: 'parquet-input-bucket-name',
            value: parquetOutputBucket.bucketName,
            stackName: id,
            description: 'Parquet Input Bucket Name',
            cfnOutput: true
        });
    }
}
