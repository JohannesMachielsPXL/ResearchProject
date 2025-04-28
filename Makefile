# Default AWS region (adjust if needed)
AWS_REGION ?= eu-west-1

.PHONY: manage clean deploy-aws-staging deploy-aws-prod destroy-aws-staging destroy-aws-prod

clean:
	rm -rf cdk.out/
	echo "Clean is finished"

deploy-aws-staging:
	cdk deploy --app "npx ts-node infrastructure/aws/bin/mdf4-insights.ts" -c stage=staging --all

deploy-aws-prod:
	cdk deploy --app "npx ts-node infrastructure/aws/bin/mdf4-insights.ts" -c stage=prod --all

destroy-aws-staging:
	cdk destroy --app "npx ts-node infrastructure/aws/bin/mdf4-insights.ts" -c stage=staging --all

destroy-aws-prod:
	cdk destroy --app "npx ts-node infrastructure/aws/bin/mdf4-insights.ts" -c stage=prod --all

# Prevent unintended target parsing
.SECONDARY:
%:
	@true
