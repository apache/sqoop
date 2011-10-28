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

package com.cloudera.sqoop.mapreduce;

/**
 * @deprecated Moving to use org.apache.sqoop namespace.
 */
public class MySQLExportMapper<KEYIN, VALIN>
    extends org.apache.sqoop.mapreduce.MySQLExportMapper<KEYIN, VALIN> {

  public static final String MYSQL_CHECKPOINT_BYTES_KEY =
      org.apache.sqoop.mapreduce.MySQLExportMapper.MYSQL_CHECKPOINT_BYTES_KEY;

  public static final long DEFAULT_CHECKPOINT_BYTES =
      org.apache.sqoop.mapreduce.MySQLExportMapper.DEFAULT_CHECKPOINT_BYTES;

}
