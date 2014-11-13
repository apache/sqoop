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
 * Enum is a set of predefined values of its own type
 *
 * JDBC Types: enum
 */

public class Enum extends AbstractComplexListType {

  public Enum(Column listType) {
    super(listType);
  }

  public Enum(String name, Column listType) {
    super(name, listType);
  }

  public Enum(String name, Boolean nullable, Column listType) {
    super(name, nullable, listType);
  }

  @Override
  public ColumnType getType() {
    return ColumnType.ENUM;
  }

  @Override
  public String toString() {
    return new StringBuilder("Enum{")
             .append(super.toString())
             .append("}").toString();
  }
}
