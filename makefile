AWS_REGION=us-east-1
ENVIRONMENT=PRODUCTION

deploy-core:

	export ENVIRONMENT=PRODUCTION  && \
	cdk bootstrap && \
	cdk deploy production-common-stack production-data-lake-stack \
	 	production-rds-stack production-dms-stack production-glue-catalog-stack production-athena-stack \
	 	production-redshift-stack production-emr-stack
		--require-approval never

deploy-airflow:

	export ENVIRONMENT=PRODUCTION && \
	cdk deploy production-airflow-stack

data:

	export ENVIRONMENT=PRODUCTION && \
	echo "Activating DMS" && \
	python3 ./scripts/trigger_dms.py && \
	echo "Inserting data to RDS" && \
	python3 ./scripts/insert_to_rds.py

destroy-stack:

	export ENVIRONMENT=PRODUCTION  && \
	cdk destroy "*" --force

