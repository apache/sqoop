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
package org.apache.sqoop.connector.matcher;

import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.DateTime;
import org.apache.sqoop.schema.type.Text;
import org.joda.time.LocalDateTime;

class SchemaFixture {

  public static Schema createSchema1(String name) {
    Schema schema = new Schema(name);
    schema.addColumn(new Text("text1"));
    schema.addColumn(new DateTime("datetime1", false, false));
    return schema;
  }

  public static Object[] createNotNullRecordForSchema1() {
    Object[] fields = new Object[2];
    fields[0] = "some text";
    fields[1] = new LocalDateTime("2015-01-01");
    return fields;
  }

  public static Schema createSchema(String name, String[] columnNames) {
    Schema schema = new Schema(name);
    for (String columnName : columnNames) {
      if (columnName.startsWith("datetime")) {
        schema.addColumn(new DateTime(columnName, false, false));
      } else {
        schema.addColumn(new Text(columnName));
      }
    }
    return schema;
  }

  public static Schema createSchema(String name, int numOfColumn) {
    Schema schema = new Schema(name);
    for (int i = 0; i < numOfColumn; i++) {
      schema.addColumn(new Text("text" + i));
    }
    return schema;
  }

}