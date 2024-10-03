# Prerequisites

In order to run this Spring Batch application, the following pre-requisites need to be met

## Java 17

Java of version 17.
To check: `java --version`

Which should give result similar to
```shell
java 17.0.9 2023-10-17 LTS
Java(TM) SE Runtime Environment (build 17.0.9+11-LTS-201)
Java HotSpot(TM) 64-Bit Server VM (build 17.0.9+11-LTS-201, mixed mode, sharing)
```

## Maven 3.9.9

Ideal version is **3.9.9**, though versions close enough to 3.9 should work as well.
To check: `mvn --version`

The desired result is similar to
```shell
------------------------------------------------------------
Apache Maven 3.9.9
------------------------------------------------------------
Java version: 17.0.2, vendor: Oracle Corporation, runtime: /home/calebesantos/dev/jdk-17.0.2
Default locale: pt_BR, platform encoding: UTF-8
OS name: "linux", version: "6.5.0-35-generic", arch: "amd64", family: "unix"
```

## Postgresql 15.x

Ideal version is **15.4** (the one it has been tested with), however versions close enough to 15.4 should work as well.
Postgres is used in this application as an input & output data source, and also in unit tests.
To check, connect to your instance: `psql -U yourusername -W` (default superuser name is **postgres**)
And in Postgres terminal, type: `select version();`

The desired result should be similar to
```shell
                                                     version                                                      
------------------------------------------------------------------------------------------------------------------
 PostgreSQL 15.4 on x86_64-apple-darwin20.6.0, compiled by Apple clang version 12.0.5 (clang-1205.0.22.9), 64-bit
(1 row)
```

**!!!** There is an important step to create 2 empty databases, for 

(1) In / out source application use:
```shell
create database mydatabaseforapp;
```

(2) In / out source unit test use:
```shell
create database mydatabasefortest;
```

## MySQL 8.0.x

Ideal version is **8.0.31** (the one it has been tested with), however versions close enough to 8.0.31 should work as well.
MySQL is used in this application as a job repository storage for Spring Batch metadata.
To check, connect to your instance: `mysql -u yourusername -h yourhostifnotlocal -p`
And in MySQL terminal, type: `select version();`

The desired result is similar to
```shell
+-----------+
| version() |
+-----------+
| 8.0.31    |
+-----------+
```

**!!!** One important step is to create empty database, which is easy to do from MySQL command-line:
```shell
create database mydatabasename;
```

# How-to

## Configurations

In order to run jobs of this Spring Batch application, there are properties that need to be set up in **src/main/resources/application.properties** files, according to DB names, usernames and passwords that you configured.
`db.job.repo.url`, `db.job.repo.username` and `db.job.repo.password` properties need to be setup to configure Spring Batch to access MySQL job repository metadata db in **src/main/resources/application.properties**. For example:
```properties
db.job.repo.url=jdbc:mysql://localhost:3306/jobrepodbname
db.job.repo.username=root
db.job.repo.password=calebepassword
```

Corresponding `db.src.url`, `db.src.username` and `db.src.password` properties need to be setup to configure Postgresql database to be an input / output source in **src/main/resources/application.properties**. For example:
```properties
db.src.url=jdbc:postgresql://127.0.0.1:5432/sourcedbname
db.src.username=postgres
db.src.password=sourcepassword
```

Already mentioned above property `db.src.url` should be setup in **src/res/test_source.properties** to configure which database of Postgresql source to use for unit test input / output. For example:
```properties
db.src.url=jdbc:postgresql://127.0.0.1:5432/testdbname
```

## Generate input data

In case all configuration properties are set properly (mentioned above), input data in Postgresql could be generated. The following property need to be set to true:
```
regenerate.bank.transaction.records=true
```

The command will delete the old data first, and then generate the new one.

## Build & run

Before running any of the jobs, the application needs to be built:
```shell
mvn clean package
```

In order to run **Bank Transaction Analysis job**, use the following property and command:

```
spring.batch.job.name=bankTransactionAnalysisJob
```

```shell
java -jar target/intermediatespringbatchapp-0.0.1-SNAPSHOT.jar
```

For the **Currency Adjustment job**, the following property and command need to be used:

```
spring.batch.job.name=currencyAdjustmentJob
```

```shell
java -jar target/intermediatespringbatchapp-0.0.1-SNAPSHOT.jar
```

Alternatively you can run it in your IDE of preference as a Spring Boot app.