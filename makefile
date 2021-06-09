AWS_REGION=us-east-1
ENVIRONMENT=PRODUCTION

deploy-core:

	export ENVIRONMENT=PRODUCTION  && \
	cdk bootstrap && \
	cdk deploy production-common-stack production-data-lake-stack \
	 	production-rds-stack production-dms-stack production-glue-stack production-athena-stack \
		--require-approval never

insert-data:




deploy-dev:

	export ENVIRONMENT=DEVELOP  && \
	cdk bootstrap && \
	cdk deploy "*" \
		--require-approval never

deploy-staging:

	export ENVIRONMENT=STAGING  && \
	cdk bootstrap && \
	cdk deploy "*" \
		--require-approval never

deploy-production:

	export ENVIRONMENT=PRODUCTION  && \
	cdk bootstrap && \
	cdk deploy "*" \
		--require-approval never

synth-production:
	export ENVIRONMENT=PRODUCTION  && \
	cdk synth

destroy-production:
	export ENVIRONMENT=PRODUCTION  && \
	cdk destroy "*" --force
