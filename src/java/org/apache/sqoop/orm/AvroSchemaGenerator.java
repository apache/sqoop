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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import org.apache.sqoop.avro.AvroUtil;

import org.codehaus.jackson.node.NullNode;

/**
 * Creates an Avro schema to represent a table from a database.
 */
public class AvroSchemaGenerator {

  public static final Log LOG =
      LogFactory.getLog(AvroSchemaGenerator.class.getName());

  private final SqoopOptions options;
  private final ConnManager connManager;
  private final String tableName;

  public AvroSchemaGenerator(final SqoopOptions opts, final ConnManager connMgr,
      final String table) {
    this.options = opts;
    this.connManager = connMgr;
    this.tableName = table;
  }

  public Schema generate(String schemaNameOverride) throws IOException {
    ClassWriter classWriter = new ClassWriter(options, connManager,
        tableName, null);
    Map<String, Integer> columnTypes = classWriter.getColumnTypes();
    String[] columnNames = classWriter.getColumnNames(columnTypes);

    List<Field> fields = new ArrayList<Field>();
    for (String columnName : columnNames) {
      String cleanedCol = AvroUtil.toAvroIdentifier(ClassWriter.toJavaIdentifier(columnName));
      int sqlType = columnTypes.get(columnName);
      Schema avroSchema = toAvroSchema(sqlType, columnName);
      Field field = new Field(cleanedCol, avroSchema, null,  NullNode.getInstance());
      field.addProp("columnName", columnName);
      field.addProp("sqlType", Integer.toString(sqlType));
      fields.add(field);
    }

    TableClassName tableClassName = new TableClassName(options);
    String shortClassName = tableClassName.getShortClassForTable(tableName);
    String avroTableName = (tableName == null ? TableClassName.QUERY_RESULT : tableName);
    String avroName = schemaNameOverride != null ? schemaNameOverride :
        (shortClassName == null ? avroTableName : shortClassName);
    String avroNamespace = tableClassName.getPackageForTable();

    String doc = "Sqoop import of " + avroTableName;
    Schema schema = Schema.createRecord(avroName, doc, avroNamespace, false);
    schema.setFields(fields);
    schema.addProp("tableName", avroTableName);
    return schema;
  }

  /**
   * Will create union, because each type is assumed to be nullable.
   *
   * @param sqlType Original SQL type (might be overridden by user)
   * @param columnName Column name from the query
   * @return Schema
   */
  public Schema toAvroSchema(int sqlType, String columnName) {
    List<Schema> childSchemas = new ArrayList<Schema>();
    childSchemas.add(Schema.create(Schema.Type.NULL));
    childSchemas.add(Schema.create(toAvroType(columnName, sqlType)));
    return Schema.createUnion(childSchemas);
  }

  public Schema toAvroSchema(int sqlType) {
    return toAvroSchema(sqlType, null);
  }

  private Type toAvroType(String columnName, int sqlType) {
    Properties mapping = options.getMapColumnJava();

    if (mapping.containsKey(columnName)) {
      String type = mapping.getProperty(columnName);
      if (LOG.isDebugEnabled()) {
        LOG.info("Overriding type of column " + columnName + " to " + type);
      }

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

    return connManager.toAvroType(tableName, columnName, sqlType);
  }
}
