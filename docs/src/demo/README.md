SQOOP ON SPARK DEMO
===================

Demo build in order to show how to import data from Mysql and export to Kafka.
Steps to run the demo:

1. Create the dataset. You can use your own database or use the dataset provided in this demo. If you want to use that datase, uncompress the orders.sql.gz file:

$ gunzip sqoop/docs/src/demo/orders.sql.gz

Create a new mysql database and import the data:

$ mysqladmin -u root -p create orders
$ mysql -u root -p orders < orders.sql

2. Start sqoop server. In the $SQOOP_HOME directory, execute the following commands to start the server and connect using the Shell

$ bin/sqoop2-server start
$ bin/sqoop2-shell

3. Configure the job

sqoop:000> create link -c generic-jdbc-connector


Name: First link

Link configuration

JDBC Driver Class: org.apache.sqoop.connector.jdbc.GenericJdbcConnector
JDBC Connection String: jdbc:mysql://localhost:3306/orders
Username: root
Password: ****
Fetch Size: 
JDBC Connection Properties: 
There are currently 1 values in the map:
protocol = tcp
entry# 

SQL Dialect

Identifier enclose: 
New link was successfully created with validation status OK and name mysql

sqoop:000> create link -c kafka-connector

Name: kafka

Link configuration

List of Kafka brokers: localhost:9092
Zookeeper address: localhost:2181

sqoop:000> create job -f mysql -t kafka

Name: spark

From database configuration

Schema name: orders
Table name: orders
Table SQL statement: 
Table column names: 
There are currently 0 values in the list:
element# 
Partition column name: order_id
Null value allowed for the partition column: 
Boundary query: 

Incremental read

Check column: 
Last value: 

To Kafka configuration

Kafka topic: sqoop

Throttling resources

Extractors: 1
Loaders: 1

Classpath configuration

Extra mapper jars: 
There are currently 0 values in the list:
element# 
New job was successfully created with validation status OK  and name spark



4. Start the job

sqoop:000> start job -name spark

5. Validate in sqoop kafka topic that mysql data have been imported

$ $KAFKA_HOME/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic sqoop

