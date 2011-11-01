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


package com.cloudera.sqoop.metastore.hsqldb;

import org.apache.hadoop.conf.Configuration;

/**
 * @deprecated Moving to use org.apache.sqoop namespace.
 */
public class HsqldbMetaStore
    extends org.apache.sqoop.metastore.hsqldb.HsqldbMetaStore {

  public static final String META_STORAGE_LOCATION_KEY =
    org.apache.sqoop.metastore.hsqldb.HsqldbMetaStore.META_STORAGE_LOCATION_KEY;
  public static final String META_SERVER_PORT_KEY =
    org.apache.sqoop.metastore.hsqldb.HsqldbMetaStore.META_SERVER_PORT_KEY;
  public static final int DEFAULT_PORT =
    org.apache.sqoop.metastore.hsqldb.HsqldbMetaStore.DEFAULT_PORT;

  public HsqldbMetaStore(Configuration config) {
    super(config);
  }

}

