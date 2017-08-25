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
package com.cloudera.sqoop.metastore;

/**
 * @deprecated Moving to use org.apache.sqoop namespace.
 */
public class GenericJobStorage
    extends org.apache.sqoop.metastore.GenericJobStorage {

  public static final String META_CONNECT_KEY =
      org.apache.sqoop.metastore.GenericJobStorage.META_CONNECT_KEY;
  public static final String META_USERNAME_KEY =
      org.apache.sqoop.metastore.GenericJobStorage.META_USERNAME_KEY;
  public static final String META_PASSWORD_KEY =
      org.apache.sqoop.metastore.GenericJobStorage.META_PASSWORD_KEY;
  public static final String ROOT_TABLE_NAME_KEY =
      org.apache.sqoop.metastore.GenericJobStorage.ROOT_TABLE_NAME_KEY;

}

