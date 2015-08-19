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
package org.apache.sqoop.common.test.db.types;

/**
 * This class provides one default type to be consumed by Types Tests.
 * Any DB provider which wants to have more types covered should provide
 * a separate class and return that instead of this class.
 */
public class DefaultTypeList extends DatabaseTypeList {
  public DefaultTypeList() {
    super();

    // Integer type
    add(DatabaseType.builder("INT")
        .addExample("-32768", Integer.valueOf(-32768), "-32768")
        .addExample("-1", Integer.valueOf(-1), "-1")
        .addExample("0", Integer.valueOf(0), "0")
        .addExample("1", Integer.valueOf(1), "1")
        .addExample("32767", Integer.valueOf(32767), "32767")
        .build());
  }
}