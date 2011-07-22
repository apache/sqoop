/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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

package com.cloudera.sqoop.metastore;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/**
 * Factory that produces the correct SessionStorage system to work with
 * a particular session descriptor.
 */
public class SessionStorageFactory {

  private Configuration conf;

  /**
   * Configuration key describing the list of SessionStorage implementations
   * to use to handle sessions.
   */
  public static final String AVAILABLE_STORAGES_KEY =
      "sqoop.session.storage.implementations";

  /** The default list of available SessionStorage implementations. */
  private static final String DEFAULT_AVAILABLE_STORAGES =
      "com.cloudera.sqoop.metastore.hsqldb.HsqldbSessionStorage,"
      + "com.cloudera.sqoop.metastore.hsqldb.AutoHsqldbStorage";

  public SessionStorageFactory(Configuration config) {
    this.conf = config;

    // Ensure that we always have an available storages list.
    if (this.conf.get(AVAILABLE_STORAGES_KEY) == null) {
      this.conf.set(AVAILABLE_STORAGES_KEY, DEFAULT_AVAILABLE_STORAGES);
    }
  }

  /**
   * Given a session descriptor, determine the correct SessionStorage
   * implementation to use to handle the session and return an instance
   * of it -- or null if no SessionStorage instance is appropriate.
   */
  public SessionStorage getSessionStorage(Map<String, String> descriptor) {
    List<SessionStorage> storages = this.conf.getInstances(
        AVAILABLE_STORAGES_KEY, SessionStorage.class);
    for (SessionStorage stor : storages) {
      if (stor.canAccept(descriptor)) {
        return stor;
      }
    }

    return null;
  }
}

