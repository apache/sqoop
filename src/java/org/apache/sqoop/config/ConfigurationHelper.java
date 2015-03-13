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

package org.apache.sqoop.config;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.GenericOptionsParser;

import com.cloudera.sqoop.mapreduce.db.DBConfiguration;

import org.apache.hadoop.util.ReflectionUtils;

/**
 * This class provides static helper methods that allow access and manipulation
 * of job configuration. It is convenient to keep such access in one place in
 * order to allow easy modifications when some of these aspects change from
 * version to version of Hadoop.
 */
public final class ConfigurationHelper {

  /**
   * We track the number of maps in local mode separately as
   * mapred.map.tasks or mapreduce.job.maps is ignored in local mode and will
   * always return 1 irrespective of what we set the value to in the
   * configuration.
   */
  public static int numLocalModeMaps = 1;
  /**
   * Set the (hinted) number of map tasks for a job.
   */
  public static void setJobNumMaps(Job job, int numMapTasks) {
    if (isLocalJobTracker(job.getConfiguration())) {
      numLocalModeMaps = numMapTasks;
    } else {
      job.getConfiguration().setInt(
        ConfigurationConstants.PROP_MAPRED_MAP_TASKS, numMapTasks);
    }
  }

  /**
   * Get the (hinted) number of map tasks for a job.
   */
  public static int getJobNumMaps(JobContext job) {
    if (isLocalJobTracker(job.getConfiguration())) {
      return numLocalModeMaps;
    } else {
      return job.getConfiguration().getInt(
        ConfigurationConstants.PROP_MAPRED_MAP_TASKS, 1);
    }
  }

  /**
   * @return the number of mapper output records from a job using its counters.
   */
  public static long getNumMapOutputRecords(Job job)
      throws IOException, InterruptedException {
    return job.getCounters().findCounter(
        ConfigurationConstants.COUNTER_GROUP_MAPRED_TASK_COUNTERS,
        ConfigurationConstants.COUNTER_MAP_OUTPUT_RECORDS).getValue();
  }

  /**
   * @return the number of mapper input records from a job using its counters.
   */
  public static long getNumMapInputRecords(Job job)
      throws IOException, InterruptedException {
    return job.getCounters().findCounter(
          ConfigurationConstants.COUNTER_GROUP_MAPRED_TASK_COUNTERS,
          ConfigurationConstants.COUNTER_MAP_INPUT_RECORDS).getValue();
  }

  /**
   * Get the (hinted) number of map tasks for a job.
   */
  public static int getConfNumMaps(Configuration conf) {
    if (isLocalJobTracker(conf)) {
      return numLocalModeMaps;
    } else {
      return conf.getInt(ConfigurationConstants.PROP_MAPRED_MAP_TASKS, 1);
    }
  }

  /**
   * Set the mapper speculative execution property for a job.
   */
  public static void setJobMapSpeculativeExecution(Job job, boolean isEnabled) {
    job.getConfiguration().setBoolean(
        ConfigurationConstants.PROP_MAPRED_MAP_TASKS_SPECULATIVE_EXEC,
        isEnabled);
  }

  /**
   * Set the reducer speculative execution property for a job.
   */
  public static void setJobReduceSpeculativeExecution(
      Job job, boolean isEnabled) {
    job.getConfiguration().setBoolean(
        ConfigurationConstants.PROP_MAPRED_REDUCE_TASKS_SPECULATIVE_EXEC,
        isEnabled);
  }

  /**
   * Sets the Jobtracker address to use for a job.
   */
  public static void setJobtrackerAddr(Configuration conf, String addr) {
    conf.set(ConfigurationConstants.PROP_MAPRED_JOB_TRACKER_ADDRESS, addr);
  }

  /**
   * @return the Configuration property identifying a DBWritable to use.
   */
  public static String getDbInputClassProperty() {
    return DBConfiguration.INPUT_CLASS_PROPERTY;
  }

  /**
   * @return the Configuration property identifying the DB username.
   */
  public static String getDbUsernameProperty() {
    return DBConfiguration.USERNAME_PROPERTY;
  }

  /**
   * @return the Configuration property identifying the DB password.
   */
  public static String getDbPasswordProperty() {
    return DBConfiguration.PASSWORD_PROPERTY;
  }

  /**
   * @return the Configuration property identifying the DB connect string.
   */
  public static String getDbUrlProperty() {
    return DBConfiguration.URL_PROPERTY;
  }

  /**
   * @return the Configuration property identifying the DB input table.
   */
  public static String getDbInputTableNameProperty() {
    return DBConfiguration.INPUT_TABLE_NAME_PROPERTY;
  }

  /**
   * @return the Configuration property specifying WHERE conditions for the
   * db table.
   */
  public static String getDbInputConditionsProperty() {
    return DBConfiguration.INPUT_CONDITIONS_PROPERTY;
  }

  /**
   * Parse arguments in 'args' via the GenericOptionsParser and
   * embed the results in the supplied configuration.
   * @param conf the configuration to populate with generic options.
   * @param args the arguments to process.
   * @return the unused args to be passed to the application itself.
   */
  public static String [] parseGenericOptions(
      Configuration conf, String [] args) throws IOException {
    // This needs to be shimmed because in Apache Hadoop this can throw
    // an IOException, but it does not do so in CDH. We just mandate in
    // this method that an IOException is possible.
    GenericOptionsParser genericParser = new GenericOptionsParser(
        conf, args);
    return genericParser.getRemainingArgs();
  }

  /**
   * Get the value of the <code>name</code> property as a <code>List</code>
   * of objects implementing the interface specified by <code>xface</code>.
   *
   * An exception is thrown if any of the classes does not exist, or if it does
   * not implement the named interface.
   *
   * @param name the property name.
   * @param xface the interface implemented by the classes named by
   *        <code>name</code>.
   * @return a <code>List</code> of objects implementing <code>xface</code>.
   */
  @SuppressWarnings("unchecked")
  public static <U> List<U> getInstances(Configuration conf,
                                              String name, Class<U> xface) {
    List<U> ret = new ArrayList<U>();
    Class<?>[] classes = conf.getClasses(name);
    for (Class<?> cl: classes) {
      if (!xface.isAssignableFrom(cl)) {
        throw new RuntimeException(cl + " does not implement " + xface);
      }
      ret.add((U) ReflectionUtils.newInstance(cl, conf));
    }
    return ret;
  }

  public static boolean isLocalJobTracker(Configuration conf) {
    // If framework is set to YARN, then we can't be running in local mode
    if ("yarn".equalsIgnoreCase(conf
      .get(ConfigurationConstants.PROP_MAPREDUCE_FRAMEWORK_NAME))) {
      return false;
    }
    String jtAddr = conf
      .get(ConfigurationConstants.PROP_MAPRED_JOB_TRACKER_ADDRESS);
    String jtAddr2 = conf
      .get(ConfigurationConstants.PROP_MAPREDUCE_JOB_TRACKER_ADDRESS);
    return (jtAddr != null && jtAddr.equals("local"))
      || (jtAddr2 != null && jtAddr2.equals("local"));
  }

  private ConfigurationHelper() {
    // Disable explicit object creation
  }
}
