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
package org.apache.sqoop.schema.type;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

/**
 * Unknown column type (internally encoded as binary)
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class Unknown extends Binary {

  /**
   * Optional JDBC type that is unknown.
   */
  Long jdbcType;

  @Override
  public ColumnType getType() {
    return ColumnType.UNKNOWN;
  }

  public Long getJdbcType() {
    return jdbcType;
  }

  public Unknown setJdbcType(Long jdbcType) {
    this.jdbcType = jdbcType;
    return this;
  }

  public Unknown(String name) {
    super(name);
  }

  public Unknown(String name, Long jdbcType) {
    super(name);
    setJdbcType(jdbcType);
  }

  public Unknown(String name, Boolean nullable) {
    super(name, nullable);
  }

  public Unknown(String name, Boolean nullable, Long jdbcType) {
    super(name, nullable);
    setJdbcType(jdbcType);
  }

}
