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
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.job.MRJobConstants;
import org.apache.sqoop.json.JSONUtils;
import org.apache.sqoop.json.util.SchemaSerialization;
import org.apache.sqoop.model.ConfigUtils;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.utils.ClassUtils;
import org.json.simple.JSONObject;

import java.io.InputStream;
import java.util.Properties;

/**
 * Helper class to store and load various information in/from MapReduce configuration
 * object and JobConf object.
 */
public final class MRConfigurationUtils {

  private static final String MR_JOB_CONFIG_CLASS_FROM_CONNECTOR_LINK = MRJobConstants.PREFIX_JOB_CONFIG + "config.class.connector.from.link";

  private static final String MR_JOB_CONFIG_CLASS_TO_CONNECTOR_LINK = MRJobConstants.PREFIX_JOB_CONFIG + "config.class.connector.to.link";

  private static final String MR_JOB_CONFIG_CLASS_FROM_CONNECTOR_JOB = MRJobConstants.PREFIX_JOB_CONFIG + "config.class.connector.from.job";

  private static final String MR_JOB_CONFIG_CLASS_TO_CONNECTOR_JOB = MRJobConstants.PREFIX_JOB_CONFIG + "config.class.connector.to.job";

  private static final String MR_JOB_CONFIG_DRIVER_CONFIG_CLASS = MRJobConstants.PREFIX_JOB_CONFIG + "config.class.driver";

  private static final String MR_JOB_CONFIG_FROM_CONNECTOR_LINK = MRJobConstants.PREFIX_JOB_CONFIG + "config.connector.from.link";

  private static final Text MR_JOB_CONFIG_FROM_CONNECTOR_LINK_KEY = new Text(MR_JOB_CONFIG_FROM_CONNECTOR_LINK);

  private static final String MR_JOB_CONFIG_TO_CONNECTOR_LINK = MRJobConstants.PREFIX_JOB_CONFIG + "config.connector.to.link";

  private static final Text MR_JOB_CONFIG_TO_CONNECTOR_LINK_KEY = new Text(MR_JOB_CONFIG_TO_CONNECTOR_LINK);

  private static final String MR_JOB_CONFIG_FROM_JOB_CONFIG = MRJobConstants.PREFIX_JOB_CONFIG + "config.connector.from.job";

  private static final Text MR_JOB_CONFIG_FROM_JOB_CONFIG_KEY = new Text(MR_JOB_CONFIG_FROM_JOB_CONFIG);

  private static final String MR_JOB_CONFIG_TO_JOB_CONFIG = MRJobConstants.PREFIX_JOB_CONFIG + "config.connector.to.job";

  private static final Text MR_JOB_CONFIG_TO_JOB_CONFIG_KEY = new Text(MR_JOB_CONFIG_TO_JOB_CONFIG);

  private static final String MR_JOB_CONFIG_DRIVER_CONFIG = MRJobConstants.PREFIX_JOB_CONFIG + "config.driver";

  private static final Text MR_JOB_CONFIG_DRIVER_CONFIG_KEY = new Text(MR_JOB_CONFIG_DRIVER_CONFIG);

  private static final String SCHEMA_FROM = MRJobConstants.PREFIX_JOB_CONFIG + "schema.connector.from";

  private static final Text SCHEMA_FROM_KEY = new Text(SCHEMA_FROM);

  private static final String SCHEMA_TO = MRJobConstants.PREFIX_JOB_CONFIG + "schema.connector.to";

  private static final Text SCHEMA_TO_KEY = new Text(SCHEMA_TO);


  /**
   * Persist Connector configuration object for link.
   *
   * @param job MapReduce job object
   * @param obj Configuration object
   */
  public static void setConnectorLinkConfig(Direction type, Job job, Object obj) {
    switch (type) {
      case FROM:
        job.getConfiguration().set(MR_JOB_CONFIG_CLASS_FROM_CONNECTOR_LINK, obj.getClass().getName());
        job.getCredentials().addSecretKey(MR_JOB_CONFIG_FROM_CONNECTOR_LINK_KEY, ConfigUtils.toJson(obj).getBytes());
        break;

      case TO:
        job.getConfiguration().set(MR_JOB_CONFIG_CLASS_TO_CONNECTOR_LINK, obj.getClass().getName());
        job.getCredentials().addSecretKey(MR_JOB_CONFIG_TO_CONNECTOR_LINK_KEY, ConfigUtils.toJson(obj).getBytes());
        break;
    }
  }

  /**
   * Persist Connector configuration objects for job.
   *
   * @param job MapReduce job object
   * @param obj Configuration object
   */
  public static void setConnectorJobConfig(Direction type, Job job, Object obj) {
    switch (type) {
      case FROM:
        job.getConfiguration().set(MR_JOB_CONFIG_CLASS_FROM_CONNECTOR_JOB, obj.getClass().getName());
        job.getCredentials().addSecretKey(MR_JOB_CONFIG_FROM_JOB_CONFIG_KEY, ConfigUtils.toJson(obj).getBytes());
        break;

      case TO:
        job.getConfiguration().set(MR_JOB_CONFIG_CLASS_TO_CONNECTOR_JOB, obj.getClass().getName());
        job.getCredentials().addSecretKey(MR_JOB_CONFIG_TO_JOB_CONFIG_KEY, ConfigUtils.toJson(obj).getBytes());
        break;
    }
  }


  /**
   * Persist driver configuration object for job.
   *
   * @param job MapReduce job object
   * @param obj Configuration object
   */
  public static void setDriverConfig(Job job, Object obj) {
    job.getConfiguration().set(MR_JOB_CONFIG_DRIVER_CONFIG_CLASS, obj.getClass().getName());
    job.getCredentials().addSecretKey(MR_JOB_CONFIG_DRIVER_CONFIG_KEY, ConfigUtils.toJson(obj).getBytes());
  }

  /**
   * Persist Connector generated schema.
   *
   * @param type  Direction of schema we are persisting
   * @param job MapReduce Job object
   * @param schema Schema
   */
  public static void setConnectorSchema(Direction type, Job job, Schema schema) {
      String jsonSchema =  SchemaSerialization.extractSchema(schema).toJSONString();
      switch (type) {
        case FROM:
          job.getCredentials().addSecretKey(SCHEMA_FROM_KEY,jsonSchema.getBytes());
          return;
        case TO:
          job.getCredentials().addSecretKey(SCHEMA_TO_KEY, jsonSchema.getBytes());
          return;
    }
  }

  /**
   * Retrieve Connector configuration object for link.
   * @param configuration MapReduce configuration object
   * @return Configuration object
   */
  public static Object getConnectorLinkConfig(Direction type, Configuration configuration) {
    switch (type) {
      case FROM:
        return loadConfiguration((JobConf) configuration, MR_JOB_CONFIG_CLASS_FROM_CONNECTOR_LINK, MR_JOB_CONFIG_FROM_CONNECTOR_LINK_KEY);

      case TO:
        return loadConfiguration((JobConf) configuration, MR_JOB_CONFIG_CLASS_TO_CONNECTOR_LINK, MR_JOB_CONFIG_TO_CONNECTOR_LINK_KEY);
    }

    return null;
  }

  /**
   * Retrieve Connector configuration object for job.
   *
   * @param configuration MapReduce configuration object
   * @return Configuration object
   */
  public static Object getConnectorJobConfig(Direction type, Configuration configuration) {
    switch (type) {
      case FROM:
        return loadConfiguration((JobConf) configuration, MR_JOB_CONFIG_CLASS_FROM_CONNECTOR_JOB, MR_JOB_CONFIG_FROM_JOB_CONFIG_KEY);

      case TO:
        return loadConfiguration((JobConf) configuration, MR_JOB_CONFIG_CLASS_TO_CONNECTOR_JOB, MR_JOB_CONFIG_TO_JOB_CONFIG_KEY);
    }

    return null;
  }

  /**
   * Retrieve Framework configuration object for job.
   *
   * @param configuration MapReduce configuration object
   * @return Configuration object
   */
  public static Object getDriverConfig(Configuration configuration) {
    return loadConfiguration((JobConf) configuration, MR_JOB_CONFIG_DRIVER_CONFIG_CLASS, MR_JOB_CONFIG_DRIVER_CONFIG_KEY);
  }



  /**
   * Retrieve Connector generated schema.
   *
   * @param type The FROM or TO connector
   * @param configuration MapReduce configuration object
   */
  public static Schema getConnectorSchema(Direction type, Configuration configuration) {
    switch (type) {
      case FROM:
        return getSchemaFromBytes(((JobConf) configuration).getCredentials().getSecretKey(SCHEMA_FROM_KEY));

      case TO:
        return getSchemaFromBytes(((JobConf) configuration).getCredentials().getSecretKey(SCHEMA_TO_KEY));
    }

    return null;
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

    JSONObject jsonSchema = JSONUtils.parse(new String(bytes));
    return SchemaSerialization.restoreSchema(jsonSchema);
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
    ConfigUtils.fillValues(json, object);

    // And give it back
    return object;
  }

  private MRConfigurationUtils() {
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
