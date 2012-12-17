= Welcome to Sqoop

Apache Sqoop is a tool designed for efficiently transferring bulk data between
Apache Hadoop and structured datastores such as relational databases. You can use
Sqoop to import data from external structured datastores into Hadoop Distributed
File System or related systems like Hive and HBase. Conversely, Sqoop can be used
to extract data from Hadoop and export it to external structured datastores such
as relational databases and enterprise data warehouses.

== Documentation

Sqoop ships with documentation, please check module "docs" for additional materials.

More documentation is available online on Sqoop home page:

http://sqoop.apache.org/

== Compiling Sqoop

Sqoop uses the Maven build system, and it can be compiled and built running the
following commands:

  mvn compile # Compile project
  mvn package # Build source artifact
  mvn package -Pbinary # Build binary artifact

Sqoop is using Sphinx plugin to generate documentation that have higher memory
requirements that might not fit into default maven configuration. You might need
to increase maximal memory allowance to successfully execute package goal. This
can done using following command:

  export MAVEN_OPTS="-Xmx512m -XX:MaxPermSize=512m"

Sqoop currently supports multiple Hadoop distributions. In order to compile Sqoop
against a specific Hadoop version, please specify the hadoop.profile property in
Maven commands. For example:

  mvn package -Pbinary -Dhadoop.profile=100

Please refer to the Sqoop documentation for a full list of supported Hadoop
distributions and values of the hadoop.profile property.
