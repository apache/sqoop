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
package org.apache.sqoop.mapreduce.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.sqoop.mapreduce.DBWritable;

import com.cloudera.sqoop.mapreduce.db.DBInputFormat.NullDBWritable;

/**
 * A container for configuration property names for jobs with DB input/output.
 *
 * The job can be configured using the static methods in this class,
 * {@link DBInputFormat}, and {@link DBOutputFormat}.
 * Alternatively, the properties can be set in the configuration with proper
 * values.
 *
 * @see DBConfiguration#configureDB(Configuration, String, String, String,
 * String)
 * @see DBInputFormat#setInput(Job, Class, String, String)
 * @see DBInputFormat#setInput(Job, Class, String, String, String, String...)
 * @see DBOutputFormat#setOutput(Job, String, String...)
 */
public class DBConfiguration {

  public static final Log LOG =
    LogFactory.getLog(DBConfiguration.class.getName());

  /** The JDBC Driver class name. */
  public static final String DRIVER_CLASS_PROPERTY =
    "mapreduce.jdbc.driver.class";

  /** JDBC Database access URL. */
  public static final String URL_PROPERTY = "mapreduce.jdbc.url";

  /** User name to access the database. */
  public static final String USERNAME_PROPERTY = "mapreduce.jdbc.username";

  /** Password to access the database. */
  public static final String PASSWORD_PROPERTY = "mapreduce.jdbc.password";
  private static final Text PASSWORD_SECRET_KEY =
    new Text(DBConfiguration.PASSWORD_PROPERTY);

  /** JDBC connection parameters. */
  public static final String CONNECTION_PARAMS_PROPERTY =
    "mapreduce.jdbc.params";

  /** Fetch size. */
  public static final String FETCH_SIZE = "mapreduce.jdbc.fetchsize";

  /** Input table name. */
  public static final String INPUT_TABLE_NAME_PROPERTY =
    "mapreduce.jdbc.input.table.name";

  /** Field names in the Input table. */
  public static final String INPUT_FIELD_NAMES_PROPERTY =
    "mapreduce.jdbc.input.field.names";

  /** WHERE clause in the input SELECT statement. */
  public static final String INPUT_CONDITIONS_PROPERTY =
    "mapreduce.jdbc.input.conditions";

  /** ORDER BY clause in the input SELECT statement. */
  public static final String INPUT_ORDER_BY_PROPERTY =
    "mapreduce.jdbc.input.orderby";

  /** Whole input query, exluding LIMIT...OFFSET. */
  public static final String INPUT_QUERY = "mapreduce.jdbc.input.query";

  /** Input query to get the count of records. */
  public static final String INPUT_COUNT_QUERY =
    "mapreduce.jdbc.input.count.query";

  /** Input query to get the max and min values of the jdbc.input.query. */
  public static final String INPUT_BOUNDING_QUERY =
      "mapred.jdbc.input.bounding.query";

  /** Class name implementing DBWritable which will hold input tuples. */
  public static final String INPUT_CLASS_PROPERTY =
    "mapreduce.jdbc.input.class";

  /** Output table name. */
  public static final String OUTPUT_TABLE_NAME_PROPERTY =
    "mapreduce.jdbc.output.table.name";

  /** Field names in the Output table. */
  public static final String OUTPUT_FIELD_NAMES_PROPERTY =
    "mapreduce.jdbc.output.field.names";

  /** Number of fields in the Output table. */
  public static final String OUTPUT_FIELD_COUNT_PROPERTY =
    "mapreduce.jdbc.output.field.count";

  /**
   * The name of the parameter to use for making Isolation level to be
   * read uncommitted by default for connections.
   */
  public static final String PROP_RELAXED_ISOLATION =
      "org.apache.sqoop.db.relaxedisolation";

  /**
   * Sets the DB access related fields in the {@link Configuration}.
   * @param conf the configuration
   * @param driverClass JDBC Driver class name
   * @param dbUrl JDBC DB access URL
   * @param userName DB access username
   * @param passwd DB access passwd
   * @param fetchSize DB fetch size
   * @param connectionParams JDBC connection parameters
   */
  public static void configureDB(Configuration conf, String driverClass,
      String dbUrl, String userName, String passwd, Integer fetchSize,
      Properties connectionParams) {

    conf.set(DRIVER_CLASS_PROPERTY, driverClass);
    conf.set(URL_PROPERTY, dbUrl);
    if (userName != null) {
      conf.set(USERNAME_PROPERTY, userName);
    }
    if (passwd != null) {
      setPassword((JobConf) conf, passwd);
    }
    if (fetchSize != null) {
      conf.setInt(FETCH_SIZE, fetchSize);
    }
    if (connectionParams != null) {
      conf.set(CONNECTION_PARAMS_PROPERTY,
               propertiesToString(connectionParams));
    }

  }

  // set the password in the secure credentials object
  private static void setPassword(JobConf configuration, String password) {
    LOG.debug("Securing password into job credentials store");
    configuration.getCredentials().addSecretKey(
      PASSWORD_SECRET_KEY, password.getBytes());
  }

  /**
   * Sets the DB access related fields in the JobConf.
   * @param job the job
   * @param driverClass JDBC Driver class name
   * @param dbUrl JDBC DB access URL
   * @param fetchSize DB fetch size
   * @param connectionParams JDBC connection parameters
   */
  public static void configureDB(Configuration job, String driverClass,
      String dbUrl, Integer fetchSize, Properties connectionParams) {
    configureDB(job, driverClass, dbUrl, null, null, fetchSize,
                connectionParams);
  }

  /**
   * Sets the DB access related fields in the {@link Configuration}.
   * @param conf the configuration
   * @param driverClass JDBC Driver class name
   * @param dbUrl JDBC DB access URL
   * @param userName DB access username
   * @param passwd DB access passwd
   * @param connectionParams JDBC connection parameters
   */
  public static void configureDB(Configuration conf, String driverClass,
      String dbUrl, String userName, String passwd,
      Properties connectionParams) {
    configureDB(conf, driverClass, dbUrl, userName, passwd, null,
                connectionParams);
  }

  /**
   * Sets the DB access related fields in the JobConf.
   * @param job the job
   * @param driverClass JDBC Driver class name
   * @param dbUrl JDBC DB access URL.
   * @param connectionParams JDBC connection parameters
   */
  public static void configureDB(Configuration job, String driverClass,
      String dbUrl, Properties connectionParams) {
    configureDB(job, driverClass, dbUrl, null, connectionParams);
  }

  /**
   * Sets the DB access related fields in the {@link Configuration}.
   * @param conf the configuration
   * @param driverClass JDBC Driver class name
   * @param dbUrl JDBC DB access URL
   * @param userName DB access username
   * @param passwd DB access passwd
   * @param fetchSize DB fetch size
   */
  public static void configureDB(Configuration conf, String driverClass,
      String dbUrl, String userName, String passwd, Integer fetchSize) {
    configureDB(conf, driverClass, dbUrl, userName, passwd, fetchSize,
                (Properties) null);
  }

  /**
   * Sets the DB access related fields in the JobConf.
   * @param job the job
   * @param driverClass JDBC Driver class name
   * @param dbUrl JDBC DB access URL
   * @param fetchSize DB fetch size
   */
  public static void configureDB(Configuration job, String driverClass,
      String dbUrl, Integer fetchSize) {
    configureDB(job, driverClass, dbUrl, fetchSize, (Properties) null);
  }

  /**
   * Sets the DB access related fields in the {@link Configuration}.
   * @param conf the configuration
   * @param driverClass JDBC Driver class name
   * @param dbUrl JDBC DB access URL
   * @param userName DB access username
   * @param passwd DB access passwd
   */
  public static void configureDB(Configuration conf, String driverClass,
      String dbUrl, String userName, String passwd) {
    configureDB(conf, driverClass, dbUrl, userName, passwd, (Properties) null);
  }

  /**
   * Sets the DB access related fields in the JobConf.
   * @param job the job
   * @param driverClass JDBC Driver class name
   * @param dbUrl JDBC DB access URL.
   */
  public static void configureDB(Configuration job, String driverClass,
      String dbUrl) {
    configureDB(job, driverClass, dbUrl, (Properties) null);
  }


  private Configuration conf;

  public DBConfiguration(Configuration job) {
    this.conf = job;
  }

  /** Returns a connection object to the DB.
   * @throws ClassNotFoundException
   * @throws SQLException */
  public Connection getConnection()
      throws ClassNotFoundException, SQLException {
    Connection connection;

    Class.forName(conf.get(DBConfiguration.DRIVER_CLASS_PROPERTY));

    String username = conf.get(DBConfiguration.USERNAME_PROPERTY);
    String password = getPassword((JobConf) conf);
    String connectString = conf.get(DBConfiguration.URL_PROPERTY);
    String connectionParamsStr =
      conf.get(DBConfiguration.CONNECTION_PARAMS_PROPERTY);
    Properties connectionParams = propertiesFromString(connectionParamsStr);

    if (connectionParams != null && connectionParams.size() > 0) {
      Properties props = new Properties();
      if (username != null) {
        props.put("user", username);
      }

      if (password != null) {
        props.put("password", password);
      }

      props.putAll(connectionParams);
      connection = DriverManager.getConnection(connectString, props);
    } else {
      if (username == null) {
        connection = DriverManager.getConnection(connectString);
      } else {
        connection = DriverManager.getConnection(
                        connectString, username, password);
      }
    }

    return connection;
  }

  // retrieve the password from the credentials object
  public static String getPassword(JobConf configuration) {
    LOG.debug("Fetching password from job credentials store");
    byte[] secret = configuration.getCredentials().getSecretKey(
      PASSWORD_SECRET_KEY);
    return secret != null ? new String(secret) : null;
  }

  public Configuration getConf() {
    return conf;
  }

  public Integer getFetchSize() {
    if (conf.get(DBConfiguration.FETCH_SIZE) == null) {
      return null;
    }
    return conf.getInt(DBConfiguration.FETCH_SIZE, 0);
  }

  public void setFetchSize(Integer fetchSize) {
    if (fetchSize != null) {
      conf.setInt(DBConfiguration.FETCH_SIZE, fetchSize);
    } else {
      conf.set(FETCH_SIZE, null);
    }
  }
  public String getInputTableName() {
    return conf.get(DBConfiguration.INPUT_TABLE_NAME_PROPERTY);
  }

  public void setInputTableName(String tableName) {
    conf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, tableName);
  }

  public String[] getInputFieldNames() {
    return conf.getStrings(DBConfiguration.INPUT_FIELD_NAMES_PROPERTY);
  }

  public void setInputFieldNames(String... fieldNames) {
    conf.setStrings(DBConfiguration.INPUT_FIELD_NAMES_PROPERTY, fieldNames);
  }

  public String getInputConditions() {
    return conf.get(DBConfiguration.INPUT_CONDITIONS_PROPERTY);
  }

  public void setInputConditions(String conditions) {
    if (conditions != null && conditions.length() > 0) {
      conf.set(DBConfiguration.INPUT_CONDITIONS_PROPERTY, conditions);
    }
  }

  public String getInputOrderBy() {
    return conf.get(DBConfiguration.INPUT_ORDER_BY_PROPERTY);
  }

  public void setInputOrderBy(String orderby) {
    if (orderby != null && orderby.length() >0) {
      conf.set(DBConfiguration.INPUT_ORDER_BY_PROPERTY, orderby);
    }
  }

  public String getInputQuery() {
    return conf.get(DBConfiguration.INPUT_QUERY);
  }

  public void setInputQuery(String query) {
    if (query != null && query.length() >0) {
      conf.set(DBConfiguration.INPUT_QUERY, query);
    }
  }

  public String getInputCountQuery() {
    return conf.get(DBConfiguration.INPUT_COUNT_QUERY);
  }

  public void setInputCountQuery(String query) {
    if (query != null && query.length() > 0) {
      conf.set(DBConfiguration.INPUT_COUNT_QUERY, query);
    }
  }

  public void setInputBoundingQuery(String query) {
    if (query != null && query.length() > 0) {
      conf.set(DBConfiguration.INPUT_BOUNDING_QUERY, query);
    }
  }

  public String getInputBoundingQuery() {
    return conf.get(DBConfiguration.INPUT_BOUNDING_QUERY);
  }

  public Class<?> getInputClass() {
    return conf.getClass(DBConfiguration.INPUT_CLASS_PROPERTY,
                         NullDBWritable.class);
  }

  public void setInputClass(Class<? extends DBWritable> inputClass) {
    conf.setClass(DBConfiguration.INPUT_CLASS_PROPERTY, inputClass,
                  DBWritable.class);
  }

  public String getOutputTableName() {
    return conf.get(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY);
  }

  public void setOutputTableName(String tableName) {
    conf.set(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY, tableName);
  }

  public String[] getOutputFieldNames() {
    return conf.getStrings(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY);
  }

  public void setOutputFieldNames(String... fieldNames) {
    conf.setStrings(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY, fieldNames);
  }

  public void setOutputFieldCount(int fieldCount) {
    conf.setInt(DBConfiguration.OUTPUT_FIELD_COUNT_PROPERTY, fieldCount);
  }

  public int getOutputFieldCount() {
    return conf.getInt(OUTPUT_FIELD_COUNT_PROPERTY, 0);
  }

  /**
   * Converts connection properties to a String to be passed to the mappers.
   * @param properties JDBC connection parameters
   * @return String to be passed to configuration
   */
  protected static String propertiesToString(Properties properties) {
    List<String> propertiesList = new ArrayList<String>(properties.size());
    for(Entry<Object, Object> property : properties.entrySet()) {
      String key = StringEscapeUtils.escapeCsv(property.getKey().toString());
      if (key.equals(property.getKey().toString()) && key.contains("=")) {
        key = "\"" + key + "\"";
      }
      String val = StringEscapeUtils.escapeCsv(property.getValue().toString());
      if (val.equals(property.getValue().toString()) && val.contains("=")) {
        val = "\"" + val + "\"";
      }
      propertiesList.add(StringEscapeUtils.escapeCsv(key + "=" + val));
    }
    return StringUtils.join(propertiesList, ',');
  }

  /**
   * Converts a String back to connection parameters.
   * @param input String from configuration
   * @return JDBC connection parameters
   */
  protected static Properties propertiesFromString(String input) {
    if (input != null && !input.isEmpty()) {
      Properties result = new Properties();
      StrTokenizer propertyTokenizer = StrTokenizer.getCSVInstance(input);
      StrTokenizer valueTokenizer = StrTokenizer.getCSVInstance();
      valueTokenizer.setDelimiterChar('=');
      while (propertyTokenizer.hasNext()){
        valueTokenizer.reset(propertyTokenizer.nextToken());
        String[] values = valueTokenizer.getTokenArray();
        if (values.length==2) {
          result.put(values[0], values[1]);
        }
      }
      return result;
    } else {
      return null;
    }
  }

}
