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

/**
 * Unsupported data type (internally encoded as binary).
 */
public class Unsupported extends Binary {

  /**
   * Optional JDBC type that is unknown.
   */
  Long jdbcType;

  @Override
  public Type getType() {
    return Type.UNSUPPORTED;
  }

  public Long getJdbcType() {
    return jdbcType;
  }

  public Unsupported setJdbcType(Long jdbcType) {
    this.jdbcType = jdbcType;
    return this;
  }

  public Unsupported() {
  }

  public Unsupported(Long jdbcType) {
    setJdbcType(jdbcType);
  }

  public Unsupported(String name) {
    super(name);
  }

  public Unsupported(String name, Long jdbcType) {
    super(name);
    setJdbcType(jdbcType);
  }

  public Unsupported(String name, Boolean nullable) {
    super(name, nullable);
  }

  public Unsupported(String name, Boolean nullable, Long jdbcType) {
    super(name, nullable);
    setJdbcType(jdbcType);
  }

}
