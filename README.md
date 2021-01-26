# Logstash Input RDS

    input {
      rds {
        region => "us-west-2"
        instance_name => "development"
        log_file_name => "error/postgresql.log"
      }
    }

## Publishing gem

```bash
$ gem build logstash-input-rds.gemspec
...
$ gem push --key github \
  --host https://rubygems.pkg.github.com/moteef \
  <the gem file generate from the build command>
```
