# Learn Spark

Project Based Learning approach to learning Apache Spark.

## Scala

Scala version 2.12.10
Spark version 3.0.1

Kudu integrates with Spark through the Data Source API. Include the `kudu-spark` dependency using the `--packages` option.

```
spark-shell --packages org.apache.kudu:kudu-spark3_2.12:1.13.0.7.1.5.0-257 --repositories https://repository.cloudera.com/artifactory/cloudera-repos/
```

We first import the kudu spark package, then create a DataFrame

```
import org.apache.kudu.spark.kudu._
```

```
val df = spark.read.options(Map("kudu.master" -> "edh-master-01kc.root.corp:7051", "kudu.table" -> "impala::de_ber_4con.can_ges_contact_center_ft")).format("kudu").load
```

## Python

Python version 2.7.5
Spark version 3.0.1

```
pyspark --packages org.apache.kudu:kudu-spark3_2.12:1.13.0.7.1.5.0-257 --repositories https://repository.cloudera.com/artifactory/cloudera-repos/
```

```
kuduDF = spark.read.format('org.apache.kudu.spark.kudu').option('kudu.master',"edh-master-01kc.root.corp:7051").option('kudu.table',"impala::de_ber_4con.can_ges_contact_center_ft").load()
```