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

package org.apache.sqoop.orm;

import java.io.IOException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import java.util.Properties;

/**
 * Creates an Avro schema to represent a table from a database.
 */
public class AvroSchemaGenerator {

  private final SqoopOptions options;
  private final ConnManager connManager;
  private final String tableName;

  public AvroSchemaGenerator(final SqoopOptions opts, final ConnManager connMgr,
      final String table) {
    this.options = opts;
    this.connManager = connMgr;
    this.tableName = table;
  }

  public Schema generate() throws IOException {
    ClassWriter classWriter = new ClassWriter(options, connManager,
        tableName, null);
    Map<String, Integer> columnTypes = classWriter.getColumnTypes();
    String[] columnNames = classWriter.getColumnNames(columnTypes);

    List<Field> fields = new ArrayList<Field>();
    for (String columnName : columnNames) {
      String cleanedCol = ClassWriter.toIdentifier(columnName);
      int sqlType = columnTypes.get(cleanedCol);
      Schema avroSchema = toAvroSchema(sqlType, columnName);
      Field field = new Field(cleanedCol, avroSchema, null, null);
      field.addProp("columnName", columnName);
      field.addProp("sqlType", Integer.toString(sqlType));
      fields.add(field);
    }

    TableClassName tableClassName = new TableClassName(options);
    String shortClassName = tableClassName.getShortClassForTable(tableName);
    String avroTableName = (tableName == null ? "QueryResult" : tableName);
    String avroName = (shortClassName == null ? avroTableName : shortClassName);
    String avroNamespace = tableClassName.getPackageForTable();

    String doc = "Sqoop import of " + avroTableName;
    Schema schema = Schema.createRecord(avroName, doc, avroNamespace, false);
    schema.setFields(fields);
    schema.addProp("tableName", avroTableName);
    return schema;
  }

  private Type toAvroType(int sqlType) {
    switch (sqlType) {
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
        return Type.INT;
      case Types.BIGINT:
        return Type.LONG;
      case Types.BIT:
      case Types.BOOLEAN:
        return Type.BOOLEAN;
      case Types.REAL:
        return Type.FLOAT;
      case Types.FLOAT:
      case Types.DOUBLE:
        return Type.DOUBLE;
      case Types.NUMERIC:
      case Types.DECIMAL:
        return Type.STRING;
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
      case Types.LONGNVARCHAR:
      case Types.NVARCHAR:
      case Types.NCHAR:
        return Type.STRING;
      case Types.DATE:
      case Types.TIME:
      case Types.TIMESTAMP:
        return Type.LONG;
      case Types.BINARY:
      case Types.VARBINARY:
        return Type.BYTES;
      default:
        throw new IllegalArgumentException("Cannot convert SQL type "
            + sqlType);
    }
  }

  private Type toAvroType(String type) {
    if (type.equalsIgnoreCase("INTEGER")) { return Type.INT; }
    if (type.equalsIgnoreCase("LONG")) { return Type.LONG; }
    if (type.equalsIgnoreCase("BOOLEAN")) { return Type.BOOLEAN; }
    if (type.equalsIgnoreCase("FLOAT")) { return Type.FLOAT; }
    if (type.equalsIgnoreCase("DOUBLE")) { return Type.DOUBLE; }
    if (type.equalsIgnoreCase("STRING")) { return Type.STRING; }
    if (type.equalsIgnoreCase("BYTES")) { return Type.BYTES; }

    // Mapping was not found
    throw new IllegalArgumentException("Cannot convert to AVRO type " + type);
  }

  /**
   * Will create union, because each type is assumed to be nullable.
   *
   * @param sqlType Original SQL type (might be overridden by user)
   * @param columnName Column name from the query
   * @return Schema
   */
  public Schema toAvroSchema(int sqlType, String columnName) {
    Properties mappingJava = options.getMapColumnJava();

    // Try to apply any user specified mapping
    Type targetType;
    if (columnName != null && mappingJava.containsKey(columnName)) {
        targetType = toAvroType((String)mappingJava.get(columnName));
    } else {
      targetType = toAvroType(sqlType);
    }

    List<Schema> childSchemas = new ArrayList<Schema>();
    childSchemas.add(Schema.create(targetType));
    childSchemas.add(Schema.create(Schema.Type.NULL));
    return Schema.createUnion(childSchemas);
  }

  public Schema toAvroSchema(int sqlType) {
    return toAvroSchema(sqlType, null);
  }
}
