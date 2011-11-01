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
package com.cloudera.sqoop.config;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * @deprecated Moving to use org.apache.sqoop namespace.
 */
public final class ConfigurationHelper {

  public static void setJobNumMaps(Job job, int numMapTasks) {
    org.apache.sqoop.config.ConfigurationHelper.setJobNumMaps(job, numMapTasks);
  }

  public static int getJobNumMaps(JobContext job) {
    return org.apache.sqoop.config.ConfigurationHelper.getJobNumMaps(job);
  }

  public static long getNumMapOutputRecords(Job job)
      throws IOException, InterruptedException {
    return org.apache.sqoop.config.
           ConfigurationHelper.getNumMapOutputRecords(job);
  }

  public static long getNumMapInputRecords(Job job)
      throws IOException, InterruptedException {
    return org.apache.sqoop.config.
            ConfigurationHelper.getNumMapInputRecords(job);
  }

  public static int getConfNumMaps(Configuration conf) {
    return org.apache.sqoop.config.ConfigurationHelper.getConfNumMaps(conf);
  }

  public static void setJobMapSpeculativeExecution(Job job, boolean isEnabled) {
    org.apache.sqoop.config.
        ConfigurationHelper.setJobMapSpeculativeExecution(job, isEnabled);
  }

  public static void setJobReduceSpeculativeExecution(
      Job job, boolean isEnabled) {
    org.apache.sqoop.config.
        ConfigurationHelper.setJobReduceSpeculativeExecution(job, isEnabled);
  }

  public static void setJobtrackerAddr(Configuration conf, String addr) {
    org.apache.sqoop.config.
        ConfigurationHelper.setJobtrackerAddr(conf, addr);
  }

  public static String getDbInputClassProperty() {
    return org.apache.sqoop.config.
               ConfigurationHelper.getDbInputClassProperty();
  }

  public static String getDbUsernameProperty() {
    return org.apache.sqoop.config.
               ConfigurationHelper.getDbUsernameProperty();
  }

  public static String getDbPasswordProperty() {
    return org.apache.sqoop.config.
               ConfigurationHelper.getDbPasswordProperty();
  }

  public static String getDbUrlProperty() {
    return org.apache.sqoop.config.
               ConfigurationHelper.getDbUrlProperty();
  }

  public static String getDbInputTableNameProperty() {
    return org.apache.sqoop.config.
               ConfigurationHelper.getDbInputTableNameProperty();
  }


  public static String getDbInputConditionsProperty() {
    return org.apache.sqoop.config.
               ConfigurationHelper.getDbInputConditionsProperty();
  }

  public static String [] parseGenericOptions(
      Configuration conf, String [] args) throws IOException {
    return org.apache.sqoop.config.
               ConfigurationHelper.parseGenericOptions(conf, args);
  }

  private ConfigurationHelper() {
    // Disable explicit object creation
  }

}
