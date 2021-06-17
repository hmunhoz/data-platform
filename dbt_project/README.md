First, we need to configure redshift's credentials in the ~/.dbt/profiles.yml file (you may need to create it manually).

```
$ mkdir ~/.dbt
```

Inside the dbt directory, create the profiles.yml file and populate it as such:

```yaml
redshift:
  target: prod
  outputs:
    prod:
      type: redshift
      threads: 2
      host: <Insert host>
      port: 5439
      user: admin
      dbname: dw
      pass: <insert password>
      schema: analytics_gold
```

Don't forget to modify the fields with their proper values. You can get them on the AWS Secrets Manager console.


### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](http://slack.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
