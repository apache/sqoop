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

import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.HsqldbTestServer;
import com.cloudera.sqoop.testutil.ImportJobTestCase;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.kitesdk.data.CompressionType;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.Datasets;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests --as-parquetfile.
 */
public class TestParquetImport extends ImportJobTestCase {

  public static final Log LOG = LogFactory
      .getLog(TestParquetImport.class.getName());

  /**
   * Create the argv to pass to Sqoop.
   *
   * @return the argv as an array of strings.
   */
  protected String[] getOutputArgv(boolean includeHadoopFlags,
          String[] extraArgs) {
    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
    }

    args.add("--table");
    args.add(getTableName());
    args.add("--connect");
    args.add(HsqldbTestServer.getUrl());
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--m");
    args.add("1");
    args.add("--split-by");
    args.add("INTFIELD1");
    args.add("--as-parquetfile");
    if (extraArgs != null) {
      args.addAll(Arrays.asList(extraArgs));
    }

    return args.toArray(new String[args.size()]);
  }

  protected String[] getOutputQueryArgv(boolean includeHadoopFlags, String[] extraArgs) {
    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
    }

    args.add("--query");
    args.add("SELECT * FROM " + getTableName() + " WHERE $CONDITIONS");
    args.add("--connect");
    args.add(HsqldbTestServer.getUrl());
    args.add("--target-dir");
    args.add(getWarehouseDir() + "/" + getTableName());
    args.add("--m");
    args.add("1");
    args.add("--split-by");
    args.add("INTFIELD1");
    args.add("--as-parquetfile");
    if (extraArgs != null) {
      args.addAll(Arrays.asList(extraArgs));
    }

    return args.toArray(new String[args.size()]);
  }

  @Test
  public void testSnappyCompression() throws IOException {
    runParquetImportTest("snappy");
  }

  @Test
  public void testDeflateCompression() throws IOException {
    runParquetImportTest("deflate");
  }

  private void runParquetImportTest(String codec) throws IOException {
    String[] types = {"BIT", "INTEGER", "BIGINT", "REAL", "DOUBLE", "VARCHAR(6)",
        "VARBINARY(2)",};
    String[] vals = {"true", "100", "200", "1.0", "2.0", "'s'", "'0102'", };
    createTableWithColTypes(types, vals);

    String [] extraArgs = { "--compression-codec", codec};
    runImport(getOutputArgv(true, extraArgs));

    assertEquals(CompressionType.forName(codec), getCompressionType());

    Schema schema = getSchema();
    assertEquals(Type.RECORD, schema.getType());
    List<Field> fields = schema.getFields();
    assertEquals(types.length, fields.size());
    checkField(fields.get(0), "DATA_COL0", Type.BOOLEAN);
    checkField(fields.get(1), "DATA_COL1", Type.INT);
    checkField(fields.get(2), "DATA_COL2", Type.LONG);
    checkField(fields.get(3), "DATA_COL3", Type.FLOAT);
    checkField(fields.get(4), "DATA_COL4", Type.DOUBLE);
    checkField(fields.get(5), "DATA_COL5", Type.STRING);
    checkField(fields.get(6), "DATA_COL6", Type.BYTES);

    DatasetReader<GenericRecord> reader = getReader();
    try {
      GenericRecord record1 = reader.next();
      assertNotNull(record1);
      assertEquals("DATA_COL0", true, record1.get("DATA_COL0"));
      assertEquals("DATA_COL1", 100, record1.get("DATA_COL1"));
      assertEquals("DATA_COL2", 200L, record1.get("DATA_COL2"));
      assertEquals("DATA_COL3", 1.0f, record1.get("DATA_COL3"));
      assertEquals("DATA_COL4", 2.0, record1.get("DATA_COL4"));
      assertEquals("DATA_COL5", "s", record1.get("DATA_COL5"));
      Object object = record1.get("DATA_COL6");
      assertTrue(object instanceof ByteBuffer);
      ByteBuffer b = ((ByteBuffer) object);
      assertEquals((byte) 1, b.get(0));
      assertEquals((byte) 2, b.get(1));
      assertFalse(reader.hasNext());
    } finally {
      reader.close();
    }
  }

  @Test
  public void testOverrideTypeMapping() throws IOException {
    String [] types = { "INT" };
    String [] vals = { "10" };
    createTableWithColTypes(types, vals);

    String [] extraArgs = { "--map-column-java", "DATA_COL0=String"};
    runImport(getOutputArgv(true, extraArgs));

    Schema schema = getSchema();
    assertEquals(Type.RECORD, schema.getType());
    List<Field> fields = schema.getFields();
    assertEquals(types.length, fields.size());
    checkField(fields.get(0), "DATA_COL0", Type.STRING);

    DatasetReader<GenericRecord> reader = getReader();
    try {
      assertTrue(reader.hasNext());
      GenericRecord record1 = reader.next();
      assertEquals("DATA_COL0", "10", record1.get("DATA_COL0"));
      assertFalse(reader.hasNext());
    } finally {
      reader.close();
    }
  }

  @Test
  public void testFirstUnderscoreInColumnName() throws IOException {
    String [] names = { "_NAME" };
    String [] types = { "INT" };
    String [] vals = { "1987" };
    createTableWithColTypesAndNames(names, types, vals);

    runImport(getOutputArgv(true, null));

    Schema schema = getSchema();
    assertEquals(Type.RECORD, schema.getType());
    List<Field> fields = schema.getFields();
    assertEquals(types.length, fields.size());
    checkField(fields.get(0), "__NAME", Type.INT);

    DatasetReader<GenericRecord> reader = getReader();
    try {
      assertTrue(reader.hasNext());
      GenericRecord record1 = reader.next();
      assertEquals("__NAME", 1987, record1.get("__NAME"));
      assertFalse(reader.hasNext());
    } finally {
      reader.close();
    }
  }

  @Test
  public void testNonIdentCharactersInColumnName() throws IOException {
    String [] names = { "test_p-a+r/quet" };
    String [] types = { "INT" };
    String [] vals = { "2015" };
    createTableWithColTypesAndNames(names, types, vals);

    runImport(getOutputArgv(true, null));

    Schema schema = getSchema();
    assertEquals(Type.RECORD, schema.getType());
    List<Field> fields = schema.getFields();
    assertEquals(types.length, fields.size());
    checkField(fields.get(0), "TEST_P_A_R_QUET", Type.INT);

    DatasetReader<GenericRecord> reader = getReader();
    try {
      assertTrue(reader.hasNext());
      GenericRecord record1 = reader.next();
      assertEquals("TEST_P_A_R_QUET", 2015, record1.get("TEST_P_A_R_QUET"));
      assertFalse(reader.hasNext());
    } finally {
      reader.close();
    }
  }

  @Test
  public void testNullableParquetImport() throws IOException, SQLException {
    String [] types = { "INT" };
    String [] vals = { null };
    createTableWithColTypes(types, vals);

    runImport(getOutputArgv(true, null));

    DatasetReader<GenericRecord> reader = getReader();
    try {
      assertTrue(reader.hasNext());
      GenericRecord record1 = reader.next();
      assertNull(record1.get("DATA_COL0"));
      assertFalse(reader.hasNext());
    } finally {
      reader.close();
    }
  }

  @Test
  public void testQueryImport() throws IOException, SQLException {
    String [] types = { "INT" };
    String [] vals = { "1" };
    createTableWithColTypes(types, vals);

    runImport(getOutputQueryArgv(true, null));

    DatasetReader<GenericRecord> reader = getReader();
    try {
      assertTrue(reader.hasNext());
      GenericRecord record1 = reader.next();
      assertEquals(1, record1.get("DATA_COL0"));
      assertFalse(reader.hasNext());
    } finally {
      reader.close();
    }
  }

  @Test
  public void testIncrementalParquetImport() throws IOException, SQLException {
    String [] types = { "INT" };
    String [] vals = { "1" };
    createTableWithColTypes(types, vals);

    runImport(getOutputArgv(true, null));
    runImport(getOutputArgv(true, new String[]{"--append"}));

    DatasetReader<GenericRecord> reader = getReader();
    try {
      assertTrue(reader.hasNext());
      GenericRecord record1 = reader.next();
      assertEquals(1, record1.get("DATA_COL0"));
      record1 = reader.next();
      assertEquals(1, record1.get("DATA_COL0"));
      assertFalse(reader.hasNext());
    } finally {
      reader.close();
    }
  }

  @Test
  public void testOverwriteParquetDatasetFail() throws IOException, SQLException {
    String [] types = { "INT" };
    String [] vals = {};
    createTableWithColTypes(types, vals);

    runImport(getOutputArgv(true, null));
    try {
      runImport(getOutputArgv(true, null));
      fail("");
    } catch (IOException ex) {
      // ok
    }
  }

  private CompressionType getCompressionType() {
    return getDataset().getDescriptor().getCompressionType();
  }

  private Schema getSchema() {
    return getDataset().getDescriptor().getSchema();
  }

  private DatasetReader<GenericRecord> getReader() {
    return getDataset().newReader();
  }

  private Dataset<GenericRecord> getDataset() {
    String uri = "dataset:file:" + getTablePath();
    return Datasets.load(uri, GenericRecord.class);
  }

  @Override
  public void tearDown() {
    super.tearDown();
    String uri = "dataset:file:" + getTablePath();
    if (Datasets.exists(uri)) {
      Datasets.delete(uri);
    }
  }

  private void checkField(Field field, String name, Type type) {
    assertEquals(name, field.name());
    assertEquals(Type.UNION, field.schema().getType());
    assertEquals(Type.NULL, field.schema().getTypes().get(0).getType());
    assertEquals(type, field.schema().getTypes().get(1).getType());
  }

}
