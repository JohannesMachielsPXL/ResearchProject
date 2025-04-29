import {RegionInfo} from '@aws-cdk/region-info';
import {Stack, StackProps, Tags} from 'aws-cdk-lib';
import {Construct} from 'constructs';

export type AppContext = {
    name: string;
    appPrefix: string;
    tagPrefix: string;
}

export type DeploymentContext = {
    region: RegionInfo;
    account?: string;
}

export type Context = {
    app: AppContext;
    deployment: DeploymentContext;
    stage: string;
    isMultiRegion?: boolean;
}

export type GlobalContext = {
    app: AppContext;
    deployment: DeploymentContext;
}

/** @internal */
export abstract class BaseStack extends Stack {
    constructor(scope: Construct, ctx: Context, stackName: string, props?: StackProps, tags?: {[key: string]: string}) {
        const regionId = ctx.isMultiRegion ? `-${ctx.deployment.region.name}` : '';
        super(scope, `${ctx.app.appPrefix}${regionId}-${ctx.stage}-${stackName}`, props);
        Tags.of(this).add(
            'AppManagerCFNStackKey', `${ctx.app.appPrefix}${regionId}-${ctx.stage}`);

        if(tags) {
            for(const [key, value] of Object.entries(tags)) {
                Tags.of(this).add(key, value);
            }
            // tags.forEach((value, key) => {
            //     this.tags.setTag(key, value, 100, true);
            // });
        }
    }
}

/** @internal */
export abstract class GlobalBaseStack extends Stack {
    constructor(scope: Construct, ctx: GlobalContext, stackName: string, props?: StackProps, tags?: {[key: string]: string}) {
        super(scope, `${ctx.app.appPrefix}-${stackName}`, props);
        Tags.of(this).add('AppManagerCFNStackKey', `${ctx.app.appPrefix}`);

        if(tags) {
            for(const [key, value] of Object.entries(tags)) {
                Tags.of(this).add(key, value);
            }
            // tags.forEach((value, key) => {
            //     this.tags.setTag(key, value, 100, true);
            // });
        }
    }
}
