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
package org.apache.sqoop.job.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.PropertyConfigurator;
import org.apache.sqoop.job.JobConstants;
import org.apache.sqoop.json.util.SchemaSerialization;
import org.apache.sqoop.model.FormUtils;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.utils.ClassUtils;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.InputStream;
import java.util.Properties;

/**
 * Helper class to store and load various information in/from MapReduce configuration
 * object and JobConf object.
 */
public final class ConfigurationUtils {

  private static final String JOB_TYPE = JobConstants.PREFIX_JOB_CONFIG + "type";

  private static final String JOB_CONFIG_CLASS_CONNECTOR_CONNECTION = JobConstants.PREFIX_JOB_CONFIG + "config.class.connector.connection";

  private static final String JOB_CONFIG_CLASS_CONNECTOR_JOB = JobConstants.PREFIX_JOB_CONFIG + "config.class.connector.job";

  private static final String JOB_CONFIG_CLASS_FRAMEWORK_CONNECTION =  JobConstants.PREFIX_JOB_CONFIG + "config.class.framework.connection";

  private static final String JOB_CONFIG_CLASS_FRAMEWORK_JOB = JobConstants.PREFIX_JOB_CONFIG + "config.class.framework.job";

  private static final String JOB_CONFIG_CONNECTOR_CONNECTION = JobConstants.PREFIX_JOB_CONFIG + "config.connector.connection";

  private static final Text JOB_CONFIG_CONNECTOR_CONNECTION_KEY = new Text(JOB_CONFIG_CONNECTOR_CONNECTION);

  private static final String JOB_CONFIG_CONNECTOR_JOB = JobConstants.PREFIX_JOB_CONFIG + "config.connector.job";

  private static final Text JOB_CONFIG_CONNECTOR_JOB_KEY = new Text(JOB_CONFIG_CONNECTOR_JOB);

  private static final String JOB_CONFIG_FRAMEWORK_CONNECTION = JobConstants.PREFIX_JOB_CONFIG + "config.framework.connection";

  private static final Text JOB_CONFIG_FRAMEWORK_CONNECTION_KEY = new Text(JOB_CONFIG_FRAMEWORK_CONNECTION);

  private static final String JOB_CONFIG_FRAMEWORK_JOB = JobConstants.PREFIX_JOB_CONFIG + "config.framework.job";

  private static final Text JOB_CONFIG_FRAMEWORK_JOB_KEY = new Text(JOB_CONFIG_FRAMEWORK_JOB);

  private static final String SCHEMA_CONNECTOR = JobConstants.PREFIX_JOB_CONFIG + "schema.connector";

  private static final Text SCHEMA_CONNECTOR_KEY = new Text(SCHEMA_CONNECTOR);

  private static final String SCHEMA_HIO = JobConstants.PREFIX_JOB_CONFIG + "schema.hio";

  private static final Text SCHEMA_HIO_KEY = new Text(SCHEMA_HIO);

  /**
   * Persist job type in the configuration object.
   *
   * @param configuration MapReduce configuration object
   * @param type Job type
   */
  public static void setJobType(Configuration configuration, MJob.Type type) {
    configuration.set(JOB_TYPE, type.name());
  }

  /**
   * Retrieve job type.
   *
   * @param configuration MapReduce configuration object
   * @return Job type
   */
  public static MJob.Type getJobType(Configuration configuration) {
    return MJob.Type.valueOf(configuration.get(JOB_TYPE));
  }

  /**
   * Persist Connector configuration object for connection.
   *
   * @param job MapReduce job object
   * @param obj Configuration object
   */
  public static void setConfigConnectorConnection(Job job, Object obj) {
    job.getConfiguration().set(JOB_CONFIG_CLASS_CONNECTOR_CONNECTION, obj.getClass().getName());
    job.getCredentials().addSecretKey(JOB_CONFIG_CONNECTOR_CONNECTION_KEY, FormUtils.toJson(obj).getBytes());
  }

  /**
   * Persist Connector configuration object for job.
   *
   * @param job MapReduce job object
   * @param obj Configuration object
   */
  public static void setConfigConnectorJob(Job job, Object obj) {
    job.getConfiguration().set(JOB_CONFIG_CLASS_CONNECTOR_JOB, obj.getClass().getName());
    job.getCredentials().addSecretKey(JOB_CONFIG_CONNECTOR_JOB_KEY, FormUtils.toJson(obj).getBytes());
  }

  /**
   * Persist Framework configuration object for connection.
   *
   * @param job MapReduce job object
   * @param obj Configuration object
   */
  public static void setConfigFrameworkConnection(Job job, Object obj) {
    job.getConfiguration().set(JOB_CONFIG_CLASS_FRAMEWORK_CONNECTION, obj.getClass().getName());
    job.getCredentials().addSecretKey(JOB_CONFIG_FRAMEWORK_CONNECTION_KEY, FormUtils.toJson(obj).getBytes());
  }

  /**
   * Persist Framework configuration object for job.
   *
   * @param job MapReduce job object
   * @param obj Configuration object
   */
  public static void setConfigFrameworkJob(Job job, Object obj) {
    job.getConfiguration().set(JOB_CONFIG_CLASS_FRAMEWORK_JOB, obj.getClass().getName());
    job.getCredentials().addSecretKey(JOB_CONFIG_FRAMEWORK_JOB_KEY, FormUtils.toJson(obj).getBytes());
  }

  /**
   * Retrieve Connector configuration object for connection.
   *
   * @param configuration MapReduce configuration object
   * @return Configuration object
   */
  public static Object getConfigConnectorConnection(Configuration configuration) {
    return loadConfiguration((JobConf) configuration, JOB_CONFIG_CLASS_CONNECTOR_CONNECTION, JOB_CONFIG_CONNECTOR_CONNECTION_KEY);
  }

  /**
   * Retrieve Connector configuration object for job.
   *
   * @param configuration MapReduce configuration object
   * @return Configuration object
   */
  public static Object getConfigConnectorJob(Configuration configuration) {
    return loadConfiguration((JobConf) configuration, JOB_CONFIG_CLASS_CONNECTOR_JOB, JOB_CONFIG_CONNECTOR_JOB_KEY);
  }

  /**
   * Retrieve Framework configuration object for connection.
   *
   * @param configuration MapReduce configuration object
   * @return Configuration object
   */
  public static Object getConfigFrameworkConnection(Configuration configuration) {
    return loadConfiguration((JobConf) configuration, JOB_CONFIG_CLASS_FRAMEWORK_CONNECTION, JOB_CONFIG_FRAMEWORK_CONNECTION_KEY);
  }

  /**
   * Retrieve Framework configuration object for job.
   *
   * @param configuration MapReduce configuration object
   * @return Configuration object
   */
  public static Object getConfigFrameworkJob(Configuration configuration) {
    return loadConfiguration((JobConf) configuration, JOB_CONFIG_CLASS_FRAMEWORK_JOB, JOB_CONFIG_FRAMEWORK_JOB_KEY);
  }

  /**
   * Persist Connector generated schema.
   *
   * @param job MapReduce Job object
   * @param schema Schema
   */
  public static void setConnectorSchema(Job job, Schema schema) {
    if(schema != null) {
      job.getCredentials().addSecretKey(SCHEMA_CONNECTOR_KEY, SchemaSerialization.extractSchema(schema).toJSONString().getBytes());
    }
  }

  /**
   * Persist Framework generated schema.
   *
   * @param job MapReduce Job object
   * @param schema Schema
   */
  public static void setHioSchema(Job job, Schema schema) {
    if(schema != null) {
      job.getCredentials().addSecretKey(SCHEMA_HIO_KEY, SchemaSerialization.extractSchema(schema).toJSONString().getBytes());
    }
  }

  /**
   * Retrieve Connector generated schema.
   *
   * @param configuration MapReduce configuration object
   * @return Schema
   */
  public static Schema getConnectorSchema(Configuration configuration) {
    return getSchemaFromBytes(((JobConf) configuration).getCredentials().getSecretKey(SCHEMA_CONNECTOR_KEY));
  }

  /**
   * Retrieve Framework generated schema.
   *
   * @param configuration MapReduce configuration object
   * @return Schema
   */
  public static Schema getHioSchema(Configuration configuration) {
    return getSchemaFromBytes(((JobConf) configuration).getCredentials().getSecretKey(SCHEMA_HIO_KEY));
  }

  /**
   * Deserialize schema from JSON encoded bytes.
   *
   * This method is null safe.
   *
   * @param bytes
   * @return
   */
  private static Schema getSchemaFromBytes(byte[] bytes) {
    if(bytes == null) {
      return null;
    }
    return SchemaSerialization.restoreSchemna((JSONObject) JSONValue.parse(new String(bytes)));
  }

  /**
   * Load configuration instance serialized in Hadoop credentials cache.
   *
   * @param configuration JobConf object associated with the job
   * @param classProperty Property with stored configuration class name
   * @param valueProperty Property with stored JSON representation of the
   *                      configuration object
   * @return New instance with loaded data
   */
  private static Object loadConfiguration(JobConf configuration, String classProperty, Text valueProperty) {
    // Create new instance of configuration class
    Object object = ClassUtils.instantiate(configuration.get(classProperty));
    if(object == null) {
      return null;
    }

    String json = new String(configuration.getCredentials().getSecretKey(valueProperty));

    // Fill it with JSON data
    FormUtils.fillValues(json, object);

    // And give it back
    return object;
  }

  private ConfigurationUtils() {
    // Instantiation is prohibited
  }

  public static void configureLogging() {
    try {
      Properties props = new Properties();
      InputStream resourceAsStream =
          SqoopMapper.class.getResourceAsStream("/META-INF/log4j.properties");
      props.load(resourceAsStream);
      PropertyConfigurator.configure(props);
    } catch (Exception e) {
      System.err.println("Encountered exception while configuring logging " +
        "for sqoop: " + e);
    }
  }
}
