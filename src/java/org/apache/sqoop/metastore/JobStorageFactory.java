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

package org.apache.sqoop.metastore;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.config.ConfigurationHelper;
import com.cloudera.sqoop.metastore.JobStorage;

/**
 * Factory that produces the correct JobStorage system to work with
 * a particular job descriptor.
 */
public class JobStorageFactory {

  private Configuration conf;

  /**
   * Configuration key describing the list of JobStorage implementations
   * to use to handle jobs.
   */
  public static final String AVAILABLE_STORAGES_KEY =
      "sqoop.job.storage.implementations";

  /** The default list of available JobStorage implementations. */
  private static final String DEFAULT_AVAILABLE_STORAGES =
      "com.cloudera.sqoop.metastore.hsqldb.HsqldbJobStorage,"
      + "com.cloudera.sqoop.metastore.hsqldb.AutoHsqldbStorage";

  public JobStorageFactory(Configuration config) {
    this.conf = config;

    // Ensure that we always have an available storages list.
    if (this.conf.get(AVAILABLE_STORAGES_KEY) == null) {
      this.conf.set(AVAILABLE_STORAGES_KEY, DEFAULT_AVAILABLE_STORAGES);
    }
  }

  /**
   * Given a storage descriptor, determine the correct JobStorage
   * implementation to use to connect to the storage resource and return an
   * instance of it -- or null if no JobStorage instance is appropriate.
   */
  public JobStorage getJobStorage(Map<String, String> descriptor) {
    List<JobStorage> storages = ConfigurationHelper.getInstances(
        conf, AVAILABLE_STORAGES_KEY, JobStorage.class);
    for (JobStorage stor : storages) {
      if (stor.canAccept(descriptor)) {
        return stor;
      }
    }

    return null;
  }
}

