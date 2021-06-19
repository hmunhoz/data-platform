First, we need to configure redshift's credentials in the ~/.dbt/profiles.yml file (you may need to create it manually).

```
$ mkdir ~/.dbt
```

Inside the `.dbt` directory, create the profiles.yml file and populate it as such:

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
If you need more assistance, take a look at the [docs](https://docs.getdbt.com/docs/introduction#database-connections).

After the proper configuration, you can run the following commands from inside this directory:
* `dbt docs serve` <- serve the documentation for local consumption
* `dbt run`        <- run the steps defined inside the _models/_ directory