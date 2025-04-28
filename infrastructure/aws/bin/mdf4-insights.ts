#!/usr/bin/env node
import {RegionInfo} from 'aws-cdk-lib/region-info';
import * as cdk from 'aws-cdk-lib';
import {Context} from "../lib/base";
import {Mdf4InsightsStack} from '../lib/mdf4-insights-stack';

const app = new cdk.App();

const stageName: string = app.node.tryGetContext("stage") as string;

const stackName = ({
    "staging": "mdf4-insights-staging",
    "prod": "mdf4-insights-prod"
} as Record<string, string>)[stageName];

const env: cdk.Environment = {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: 'eu-west-1',
}

let ctx: Context = {
    app: {
        name: "app",
        appPrefix: "mdf4-insights",
        tagPrefix: "mdf4-insights",
    },
    deployment: {
        region: RegionInfo.get(env.region!),
        account: env.account
    },
    stage: stageName
}

new Mdf4InsightsStack(app, ctx, `mdf4-insights-${app.node.tryGetContext("stage")}`, {
    env,
    stackName: stackName,
    inputBucketLifecycleRules: [{
        id: 'AutoDeleteAfter30Days',
        enabled: true,
        expiration: cdk.Duration.days(30),
    }]
});
