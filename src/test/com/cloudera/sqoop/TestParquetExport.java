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

package com.cloudera.sqoop;

import com.cloudera.sqoop.testutil.ExportJobTestCase;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.kitesdk.data.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

/**
 * Test that we can export Parquet Data Files from HDFS into databases.
 */
public class TestParquetExport extends ExportJobTestCase {

  /**
   * @return an argv for the CodeGenTool to use when creating tables to export.
   */
  protected String [] getCodeGenArgv(String... extraArgs) {
    List<String> codeGenArgv = new ArrayList<String>();

    if (null != extraArgs) {
      for (String arg : extraArgs) {
        codeGenArgv.add(arg);
      }
    }

    codeGenArgv.add("--table");
    codeGenArgv.add(getTableName());
    codeGenArgv.add("--connect");
    codeGenArgv.add(getConnectString());

    return codeGenArgv.toArray(new String[0]);
  }

  /** When generating data for export tests, each column is generated
      according to a ColumnGenerator. Methods exist for determining
      what to put into Parquet objects in the files to export, as well
      as what the object representation of the column as returned by
      the database should look like.
    */
  public interface ColumnGenerator {
    /** For a row with id rowNum, what should we write into that
        Parquet record to export?
      */
    Object getExportValue(int rowNum);

    /** Return the Parquet schema for the field. */
    Schema getColumnParquetSchema();

    /** For a row with id rowNum, what should the database return
        for the given column's value?
      */
    Object getVerifyValue(int rowNum);

    /** Return the column type to put in the CREATE TABLE statement. */
    String getColumnType();
  }

  private ColumnGenerator colGenerator(final Object exportValue,
      final Schema schema, final Object verifyValue,
      final String columnType) {
    return new ColumnGenerator() {
      @Override
      public Object getVerifyValue(int rowNum) {
        return verifyValue;
      }
      @Override
      public Object getExportValue(int rowNum) {
        return exportValue;
      }
      @Override
      public String getColumnType() {
        return columnType;
      }
      @Override
      public Schema getColumnParquetSchema() {
        return schema;
      }
    };
  }

  /**
   * Create a data file that gets exported to the db.
   * @param fileNum the number of the file (for multi-file export)
   * @param numRecords how many records to write to the file.
   */
  protected void createParquetFile(int fileNum, int numRecords,
      ColumnGenerator... extraCols) throws IOException {

    String uri = "dataset:file:" + getTablePath();
    Schema schema = buildSchema(extraCols);
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
      .schema(schema)
      .format(Formats.PARQUET)
      .build();
    Dataset dataset = Datasets.create(uri, descriptor);
    DatasetWriter writer = dataset.newWriter();
    try {
      for (int i = 0; i < numRecords; i++) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", i);
        record.put("msg", getMsgPrefix() + i);
        addExtraColumns(record, i, extraCols);
        writer.write(record);
      }
    } finally {
      writer.close();
    }
  }

  private Schema buildSchema(ColumnGenerator... extraCols) {
    List<Field> fields = new ArrayList<Field>();
    fields.add(buildField("id", Schema.Type.INT));
    fields.add(buildField("msg", Schema.Type.STRING));
    int colNum = 0;
    for (ColumnGenerator gen : extraCols) {
      if (gen.getColumnParquetSchema() != null) {
        fields.add(buildParquetField(forIdx(colNum++),
            gen.getColumnParquetSchema()));
      }
    }
    Schema schema = Schema.createRecord("myschema", null, null, false);
    schema.setFields(fields);
    return schema;
  }

  private void addExtraColumns(GenericRecord record, int rowNum,
      ColumnGenerator[] extraCols) {
    int colNum = 0;
    for (ColumnGenerator gen : extraCols) {
      if (gen.getColumnParquetSchema() != null) {
        record.put(forIdx(colNum++), gen.getExportValue(rowNum));
      }
    }
  }

  private Field buildField(String name, Schema.Type type) {
    return new Field(name, Schema.create(type), null, null);
  }

  private Field buildParquetField(String name, Schema schema) {
    return new Field(name, schema, null, null);
  }

  /** Return the column name for a column index.
   *  Each table contains two columns named 'id' and 'msg', and then an
   *  arbitrary number of additional columns defined by ColumnGenerators.
   *  These columns are referenced by idx 0, 1, 2...
   *  @param idx the index of the ColumnGenerator in the array passed to
   *   createTable().
   *  @return the name of the column
   */
  protected String forIdx(int idx) {
    return "col" + idx;
  }

  /**
   * Return a SQL statement that drops a table, if it exists.
   * @param tableName the table to drop.
   * @return the SQL statement to drop that table.
   */
  protected String getDropTableStatement(String tableName) {
    return "DROP TABLE " + tableName + " IF EXISTS";
  }

  /** Create the table definition to export to, removing any prior table.
      By specifying ColumnGenerator arguments, you can add extra columns
      to the table of arbitrary type.
   */
  private void createTable(ColumnGenerator... extraColumns)
      throws SQLException {
    Connection conn = getConnection();
    PreparedStatement statement = conn.prepareStatement(
        getDropTableStatement(getTableName()),
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    try {
      statement.executeUpdate();
      conn.commit();
    } finally {
      statement.close();
    }

    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ");
    sb.append(getTableName());
    sb.append(" (id INT NOT NULL PRIMARY KEY, msg VARCHAR(64)");
    int colNum = 0;
    for (ColumnGenerator gen : extraColumns) {
      if (gen.getColumnType() != null) {
        sb.append(", " + forIdx(colNum++) + " " + gen.getColumnType());
      }
    }
    sb.append(")");

    statement = conn.prepareStatement(sb.toString(),
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    try {
      statement.executeUpdate();
      conn.commit();
    } finally {
      statement.close();
    }
  }

  /** Verify that on a given row, a column has a given value.
   * @param id the id column specifying the row to test.
   */
  private void assertColValForRowId(int id, String colName, Object expectedVal)
      throws SQLException {
    Connection conn = getConnection();
    LOG.info("Verifying column " + colName + " has value " + expectedVal);

    PreparedStatement statement = conn.prepareStatement(
        "SELECT " + colName + " FROM " + getTableName() + " WHERE id = " + id,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    Object actualVal = null;
    try {
      ResultSet rs = statement.executeQuery();
      try {
        rs.next();
        actualVal = rs.getObject(1);
      } finally {
        rs.close();
      }
    } finally {
      statement.close();
    }

    if (expectedVal != null && expectedVal instanceof byte[]) {
      assertArrayEquals((byte[]) expectedVal, (byte[]) actualVal);
    } else {
      assertEquals("Got unexpected column value", expectedVal, actualVal);
    }
  }

  /** Verify that for the max and min values of the 'id' column, the values
      for a given column meet the expected values.
   */
  protected void assertColMinAndMax(String colName, ColumnGenerator generator)
      throws SQLException {
    Connection conn = getConnection();
    int minId = getMinRowId(conn);
    int maxId = getMaxRowId(conn);

    LOG.info("Checking min/max for column " + colName + " with type "
        + generator.getColumnType());

    Object expectedMin = generator.getVerifyValue(minId);
    Object expectedMax = generator.getVerifyValue(maxId);

    assertColValForRowId(minId, colName, expectedMin);
    assertColValForRowId(maxId, colName, expectedMax);
  }

  public void testSupportedParquetTypes() throws IOException, SQLException {
    String[] argv = {};
    final int TOTAL_RECORDS = 1 * 10;

    byte[] b = new byte[] { (byte) 1, (byte) 2 };
    Schema fixed = Schema.createFixed("myfixed", null, null, 2);
    Schema enumeration = Schema.createEnum("myenum", null, null,
        Lists.newArrayList("a", "b"));

    ColumnGenerator[] gens = new ColumnGenerator[] {
      colGenerator(true, Schema.create(Schema.Type.BOOLEAN), true, "BIT"),
      colGenerator(100, Schema.create(Schema.Type.INT), 100, "INTEGER"),
      colGenerator(200L, Schema.create(Schema.Type.LONG), 200L, "BIGINT"),
      // HSQLDB maps REAL to double, not float:
      colGenerator(1.0f, Schema.create(Schema.Type.FLOAT), 1.0d, "REAL"),
      colGenerator(2.0d, Schema.create(Schema.Type.DOUBLE), 2.0d, "DOUBLE"),
      colGenerator("s", Schema.create(Schema.Type.STRING), "s", "VARCHAR(8)"),
      colGenerator(ByteBuffer.wrap(b), Schema.create(Schema.Type.BYTES),
          b, "VARBINARY(8)"),
      colGenerator(new GenericData.Fixed(fixed, b), fixed,
          b, "BINARY(2)"),
      colGenerator(new GenericData.EnumSymbol(enumeration, "a"), enumeration,
          "a", "VARCHAR(8)"),
    };
    createParquetFile(0, TOTAL_RECORDS, gens);
    createTable(gens);
    runExport(getArgv(true, 10, 10, newStrArray(argv, "-m", "" + 1)));
    verifyExport(TOTAL_RECORDS);
    for (int i = 0; i < gens.length; i++) {
      assertColMinAndMax(forIdx(i), gens[i]);
    }
  }

  public void testNullableField() throws IOException, SQLException {
    String[] argv = {};
    final int TOTAL_RECORDS = 1 * 10;

    List<Schema> childSchemas = new ArrayList<Schema>();
    childSchemas.add(Schema.create(Schema.Type.NULL));
    childSchemas.add(Schema.create(Schema.Type.STRING));
    Schema schema =  Schema.createUnion(childSchemas);
    ColumnGenerator gen0 = colGenerator(null, schema, null, "VARCHAR(64)");
    ColumnGenerator gen1 = colGenerator("s", schema, "s", "VARCHAR(64)");
    createParquetFile(0, TOTAL_RECORDS, gen0, gen1);
    createTable(gen0, gen1);
    runExport(getArgv(true, 10, 10, newStrArray(argv, "-m", "" + 1)));
    verifyExport(TOTAL_RECORDS);
    assertColMinAndMax(forIdx(0), gen0);
    assertColMinAndMax(forIdx(1), gen1);
  }

  public void testParquetRecordsNotSupported() throws IOException, SQLException {
    String[] argv = {};
    final int TOTAL_RECORDS = 1;

    Schema schema =  Schema.createRecord("nestedrecord", null, null, false);
    schema.setFields(Lists.newArrayList(buildField("myint",
        Schema.Type.INT)));
    GenericRecord record = new GenericData.Record(schema);
    record.put("myint", 100);
    // DB type is not used so can be anything:
    ColumnGenerator gen = colGenerator(record, schema, null, "VARCHAR(64)");
    createParquetFile(0, TOTAL_RECORDS,  gen);
    createTable(gen);
    try {
      runExport(getArgv(true, 10, 10, newStrArray(argv, "-m", "" + 1)));
      fail("Parquet records can not be exported.");
    } catch (Exception e) {
      // expected
      assertTrue(true);
    }
  }

  public void testMissingDatabaseFields() throws IOException, SQLException {
    String[] argv = {};
    final int TOTAL_RECORDS = 1;

    // null column type means don't create a database column
    // the Parquet value will not be exported
    ColumnGenerator gen = colGenerator(100, Schema.create(Schema.Type.INT),
        null, null);
    createParquetFile(0, TOTAL_RECORDS, gen);
    createTable(gen);
    runExport(getArgv(true, 10, 10, newStrArray(argv, "-m", "" + 1)));
    verifyExport(TOTAL_RECORDS);
  }

  public void testMissingParquetFields()  throws IOException, SQLException {
    String[] argv = {};
    final int TOTAL_RECORDS = 1;

    // null Parquet schema means don't create an Parquet field
    ColumnGenerator gen = colGenerator(null, null, null, "VARCHAR(64)");
    createParquetFile(0, TOTAL_RECORDS, gen);
    createTable(gen);
    try {
      runExport(getArgv(true, 10, 10, newStrArray(argv, "-m", "" + 1)));
      fail("Missing Parquet field.");
    } catch (Exception e) {
      // expected
      assertTrue(true);
    }
  }

}
