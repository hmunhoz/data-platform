AWS_REGION=us-east-1
STACK_NAME=data-science-fraud-detection
BUCKET_NAME=ebanx-data-science


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
