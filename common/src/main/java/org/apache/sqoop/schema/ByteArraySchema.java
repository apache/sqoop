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
package org.apache.sqoop.schema;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.schema.type.Binary;

/***
 * Schema holding a single field of Binary data Used to support connectors to
 * schemaless / unstructured systems Such as HDFS or Kafka
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ByteArraySchema extends Schema {

  private static final String BYTE_ARRAY_SCHEMA_NAME = "ByteArraySchema";
  private static final String BYTE_ARRAY_COLUMN_NAME = "ByteArraySchema_Bytes";

  public static final ByteArraySchema instance = (ByteArraySchema) new ByteArraySchema()
      .addColumn(new Binary(BYTE_ARRAY_COLUMN_NAME));

  public static ByteArraySchema getInstance() {
    return instance;
  }

  // To avoid instantiation
  private ByteArraySchema() {
    super(BYTE_ARRAY_SCHEMA_NAME);
  }
}
