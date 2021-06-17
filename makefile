AWS_REGION=us-east-1
ENVIRONMENT=PRODUCTION

deploy-core:

	export ENVIRONMENT=PRODUCTION  && \
	cdk bootstrap && \
	cdk deploy production-common-stack production-data-lake-stack \
	 	production-rds-stack production-dms-stack production-glue-stack production-athena-stack \
	 	production-redshift-stack production-emr-stack
		--require-approval never

deploy-airflow:

	export ENVIRONMENT=PRODUCTION && \
	cdk deploy production-airflow-stack

destroy-stack:

	export ENVIRONMENT=PRODUCTION  && \
	cdk destroy "*" --force

