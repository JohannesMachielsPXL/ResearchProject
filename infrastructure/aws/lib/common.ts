import {CfnOutput} from 'aws-cdk-lib';
import * as iam from 'aws-cdk-lib/aws-iam';
import {ParameterTier, StringParameter} from 'aws-cdk-lib/aws-ssm';
import {AwsCustomResource, AwsCustomResourcePolicy, PhysicalResourceId} from 'aws-cdk-lib/custom-resources';
import {Construct} from 'constructs';
import slugify from 'slugify';
import {Context, GlobalContext} from './base';


export function getGlobalName(ctx: GlobalContext | Context, name: string) {
    if ('stage' in ctx) {
        const regionId = ctx.isMultiRegion ? `-${ctx.deployment.region.name}` : '';
        return `${ctx.app.appPrefix}${regionId}-${ctx.stage}-${name}`;
    }
    return `${ctx.app.appPrefix}-${name}`;
}


export type StackOutputParameterProps = {
    id: string;
    key: string;
    value: string;
    description?: string;
    stackName?: string;
    cfnOutput?: boolean;
    ssmParameter?: boolean;
}

export function getParameterPrefix(ctx: Context) {
    const regionId = ctx.isMultiRegion ? `/${ctx.deployment.region.name}` : '';
    return `/${ctx.app.appPrefix}${regionId}/${ctx.stage}`;
}

export function createStackOutputParameter(scope: Construct, ctx: Context, props: StackOutputParameterProps) {
    const stackName = 'stackName' in props ? props.stackName : undefined;
    const description = 'description' in props && props.description ? props.description : undefined;
    const createCfnOutput = 'cfnOutput' in props ? props.cfnOutput : false;
    const createSsmParameter = 'ssmParameter' in props ? props.ssmParameter : true;

    if (createCfnOutput) {
        new CfnOutput(scope, `CfnOutput${props.id}`, {
            description: description,
            value: props.value,
            exportName: getGlobalName(ctx, props.key)
        });
    }
    if (createSsmParameter) {
        let parameterPrefix = getParameterPrefix(ctx);
        if (stackName) {
            parameterPrefix = `${parameterPrefix}/${stackName}`;
        }
        new StringParameter(scope, `SsmParam${props.id}`, {
            parameterName: `${parameterPrefix}/${props.key}`,
            stringValue: props.value,
            description: description,
            tier: ParameterTier.STANDARD,
            allowedPattern: '.*'
        });
    }
}

export function getDeviceModelKey(make: string, model: string) {
    return slugify(`${make} ${model}`, {
        lower: true,
        strict: true
    });
}

export function getDeviceParameterKey(ctx: Context, make: string, model: string, parameterName: string) {
    const deviceModelKey = getDeviceModelKey(make, model);
    return `/${ctx.app.appPrefix}/${ctx.deployment.region.name}/${ctx.stage}/${deviceModelKey}-${parameterName}`;
}

export function getBaseTags(ctx: Context) {
    const result: { [key: string]: string } = {};
    result[`${ctx.app.tagPrefix}:service`] = ctx.app.appPrefix;
    result[`${ctx.app.tagPrefix}:environment`] = ctx.stage;
    return result;
}

export function grantAccessToParameters(scope: Construct, ctx: Context, role: iam.IRole) {
    role.addManagedPolicy(new iam.ManagedPolicy(scope, 'ssm-policy', {
        managedPolicyName: getGlobalName(ctx, 'ssm-policy'),
        statements: [
            new iam.PolicyStatement({
                effect: iam.Effect.ALLOW,
                actions: [
                    'ssm:GetParameter'
                ],
                resources: [
                    `arn:aws:ssm:${ctx.deployment.region.name}:${ctx.deployment.account}:parameter/${ctx.app.appPrefix}/${ctx.stage}/*/*`
                ]
            })
        ]
    }));

}

export function grantAccessToSecrets(scope: Construct, ctx: Context, role: iam.IRole) {
    role.addManagedPolicy(new iam.ManagedPolicy(scope, 'secrets-policy', {
        managedPolicyName: getGlobalName(ctx, 'secrets-policy'),
        statements: [
            new iam.PolicyStatement({
                effect: iam.Effect.ALLOW,
                actions: [
                    'secretsmanager:GetSecretValue'
                ],
                resources: [
                    `arn:aws:secretsmanager:${ctx.deployment.region.name}:${ctx.deployment.account}:secret:${ctx.app.appPrefix}-${ctx.stage}-*`
                ]
            })
        ]
    }));
}

interface SSMParameterReaderProps {
    readonly parameterName: string;
    readonly region: string;
}

export class SSMParameterReader extends AwsCustomResource {
    constructor(scope: Construct, name: string, props: SSMParameterReaderProps) {
        const {parameterName, region} = props;

        super(scope, name, {
            onUpdate: {
                action: 'getParameter',
                service: 'SSM',
                parameters: {
                    Name: parameterName
                },
                region,
                physicalResourceId: PhysicalResourceId.of(name)
            },
            policy: AwsCustomResourcePolicy.fromSdkCalls({
                resources: AwsCustomResourcePolicy.ANY_RESOURCE
            })
        });
    }

    public getParameterValue(): string {
        return this.getResponseFieldReference('Parameter.Value').toString();
    }
}
