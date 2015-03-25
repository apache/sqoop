/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sqoop.test.minicluster;

import java.net.URL;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.test.utils.NetworkUtils;
import org.codehaus.cargo.container.ContainerType;
import org.codehaus.cargo.container.InstalledLocalContainer;
import org.codehaus.cargo.container.configuration.ConfigurationType;
import org.codehaus.cargo.container.configuration.LocalConfiguration;
import org.codehaus.cargo.container.deployable.WAR;
import org.codehaus.cargo.container.installer.Installer;
import org.codehaus.cargo.container.installer.ZipURLInstaller;
import org.codehaus.cargo.container.property.ServletPropertySet;
import org.codehaus.cargo.generic.DefaultContainerFactory;
import org.codehaus.cargo.generic.configuration.DefaultConfigurationFactory;

/**
 * Embedded tomcat Sqoop server mini cluster.
 *
 * This mini cluster will start up embedded tomcat
 */
public class TomcatSqoopMiniCluster extends SqoopMiniCluster {

  private InstalledLocalContainer container = null;
  private Integer port;

  private static final Logger LOG = Logger.getLogger(TomcatSqoopMiniCluster.class);

  /** {@inheritDoc} */
  public TomcatSqoopMiniCluster(String temporaryPath) throws Exception {
    super(temporaryPath);
    port = NetworkUtils.findAvailablePort();
  }

  /** {@inheritDoc} */
  public TomcatSqoopMiniCluster(String temporaryPath, Configuration configuration) throws Exception {
    super(temporaryPath, configuration);
    port = NetworkUtils.findAvailablePort();
  }

  /** {@inheritDoc} */
  @Override
  public void start() throws Exception {
    // Container has already been started
    if(container != null) {
      return;
    }

    prepareTemporaryPath();

    // TODO(jarcec): We should parametrize those paths, version, etc...
    // Source: http://cargo.codehaus.org/Functional+testing
    Installer installer = new ZipURLInstaller(new URL("http://archive.apache.org/dist/tomcat/tomcat-6/v6.0.36/bin/apache-tomcat-6.0.36.zip"));
    installer.install();

    LocalConfiguration configuration = (LocalConfiguration) new DefaultConfigurationFactory().createConfiguration("tomcat6x", ContainerType.INSTALLED, ConfigurationType.STANDALONE);
    container = (InstalledLocalContainer) new DefaultContainerFactory().createContainer("tomcat6x", ContainerType.INSTALLED, configuration);

    // Set home to our installed tomcat instance
    container.setHome(installer.getHome());

    // Store tomcat logs into file as they are quite handy for debugging
    container.setOutput(getTemporaryPath() + "/log/tomcat.log");

    // Propagate system properties to the container
    Map<String, String> map = new HashMap<String, String>((Map) System.getProperties());
    container.setSystemProperties(map);

    // Propagate Hadoop jars to the container classpath
    // In real world, they would be installed manually by user
    List<String> extraClassPath = new LinkedList<String>();
    String []classpath = System.getProperty("java.class.path").split(":");
    for(String jar : classpath) {
      if(jar.contains("hadoop-")      || // Hadoop jars
         jar.contains("hive-")        || // Hive jars
         jar.contains("commons-")     || // Apache Commons libraries
         jar.contains("httpcore-")    || // Apache Http Core libraries
         jar.contains("httpclient-")  || // Apache Http Client libraries
         jar.contains("htrace-")      || // htrace-core libraries, new added in Hadoop 2.6.0
         jar.contains("zookeeper-")   || // zookeeper libraries, new added in Hadoop 2.6.0
         jar.contains("curator-")     || // curator libraries, new added in Hadoop 2.6.0
         jar.contains("log4j-")       || // Log4j
         jar.contains("slf4j-")       || // Slf4j
         jar.contains("jackson-")     || // Jackson
         jar.contains("derby")        || // Derby drivers
         jar.contains("avro-")        || // Avro
         jar.contains("parquet-")     || // Parquet
         jar.contains("mysql")        || // MySQL JDBC driver
         jar.contains("postgre")      || // PostgreSQL JDBC driver
         jar.contains("oracle")       || // Oracle driver
         jar.contains("terajdbc")     || // Teradata driver
         jar.contains("tdgs")         || // Teradata driver
         jar.contains("nzjdbc")       || // Netezza driver
         jar.contains("sqljdbc")      || // Microsoft SQL Server driver
         jar.contains("libfb303")     || // Facebook thrift lib
         jar.contains("datanucleus-") || // Data nucleus libs
         jar.contains("google")          // Google libraries (guava, ...)
       ) {
        extraClassPath.add(jar);
      }
    }

    container.setExtraClasspath(extraClassPath.toArray(new String[extraClassPath.size()]));

    // Finally deploy Sqoop server war file
    configuration.addDeployable(new WAR("../server/target/sqoop.war"));

    // Random port
    configuration.setProperty(ServletPropertySet.PORT, port.toString());

    // Start Sqoop server
    LOG.info("Starting tomcat server on port " + port);
    container.start();
  }

  /** {@inheritDoc} */
  @Override
  public void stop() throws Exception {
    container.stop();
    container = null;
  }


  /**
   * Return properties for logger configuration.
   *
   * Tomcat implementation will log into log file instead of console.
   *
   * @return
   */
  protected Map<String, String> getLoggerConfiguration() {
    Map<String, String> properties = new HashMap<String, String>();

    properties.put("org.apache.sqoop.log4j.appender.file", "org.apache.log4j.RollingFileAppender");
    properties.put("org.apache.sqoop.log4j.appender.file.File", getLogPath() + "sqoop.log");
    properties.put("org.apache.sqoop.log4j.appender.file.MaxFileSize", "25MB");
    properties.put("org.apache.sqoop.log4j.appender.file.MaxBackupIndex", "5");
    properties.put("org.apache.sqoop.log4j.appender.file.layout", "org.apache.log4j.PatternLayout");
    properties.put("org.apache.sqoop.log4j.appender.file.layout.ConversionPattern", "%d{ISO8601} %-5p %c{2} [%l] %m%n\\n");
    properties.put("org.apache.sqoop.log4j.debug", "true");
    properties.put("org.apache.sqoop.log4j.rootCategory", "WARN, file");
    properties.put("org.apache.sqoop.log4j.category.org.apache.sqoop", "DEBUG");
    properties.put("org.apache.sqoop.log4j.category.org.apache.derby", "INFO");

    return properties;
  }

  /**
   * Return server URL.
   */
  public String getServerUrl() {
    // We're not doing any changes, so return default URL
    return "http://localhost:" + port + "/sqoop/";
  }
}
