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
import org.apache.sqoop.common.ErrorCode;

/**
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public enum SchemaError implements ErrorCode {

  SCHEMA_0000("Unknown error"),

  SCHEMA_0001("Column without name"),

  SCHEMA_0002("Duplicate column name"),

  SCHEMA_0003("Source and Target schemas don't match"),

  SCHEMA_0004("Non-null target column has no matching source column"),

  SCHEMA_0005("No matching method available for source and target schemas"),

  SCHEMA_0006("Schema without name"),

  SCHEMA_0007("Unknown column name"),

  ;

  private final String message;

  private SchemaError(String message) {
    this.message = message;
  }

  public String getCode() {
    return name();
  }

  public String getMessage() {
    return message;
  }
}
