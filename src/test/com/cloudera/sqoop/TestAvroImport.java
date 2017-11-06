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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.cloudera.sqoop.testutil.BaseSqoopTestCase;
import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.HsqldbTestServer;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests --as-avrodatafile.
 */
public class TestAvroImport extends ImportJobTestCase {

  public static final Log LOG = LogFactory
      .getLog(TestAvroImport.class.getName());

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
    args.add("-m");
    args.add("1");
    args.add("--table");
    args.add(getTableName());
    args.add("--connect");
    args.add(HsqldbTestServer.getUrl());
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--split-by");
    args.add("INTFIELD1");
    args.add("--as-avrodatafile");
    if (extraArgs != null) {
      args.addAll(Arrays.asList(extraArgs));
    }

    return args.toArray(new String[0]);
  }

  @Test
  public void testAvroImport() throws IOException {
    this.setCurTableName("Avro_Import_Test");
    avroImportTestHelper(null, null);
  }

  @Test
  public void testDeflateCompressedAvroImport() throws IOException {
    this.setCurTableName("Deflate_Compressed_Avro_Import_Test_1");
    avroImportTestHelper(new String[] {"--compression-codec",
      "org.apache.hadoop.io.compress.DefaultCodec", }, "deflate");
  }

  @Test
  public void testDefaultCompressedAvroImport() throws IOException {
    this.setCurTableName("Deflate_Compressed_Avro_Import_Test_2");
    avroImportTestHelper(new String[] {"--compress", }, "deflate");
  }

  @Test
  public void testUnsupportedCodec() throws IOException {
    try {
      this.setCurTableName("Deflate_Compressed_Avro_Import_Test_3");
      avroImportTestHelper(new String[] {"--compression-codec", "foobar", },
        null);
      fail("Expected IOException");
    } catch (IOException e) {
      // Exception is expected
    }
  }

  /**
   * Helper method that runs an import using Avro with optional command line
   * arguments and checks that the created file matches the expectations.
   * <p/>
   * This can be used to test various extra options that are implemented for
   * the Avro input.
   *
   * @param extraArgs extra command line arguments to pass to Sqoop in addition
   *                  to those that {@link #getOutputArgv(boolean, String[])}
   *                  returns
   */
  protected void avroImportTestHelper(String[] extraArgs, String codec)
      throws IOException {
    String[] types =
      {"BIT", "INTEGER", "BIGINT", "REAL", "DOUBLE", "VARCHAR(6)",
        "VARBINARY(2)", "DECIMAL(3,2)"};
    String[] vals = {"true", "100", "200", "1.0", "2.0", "'s'", "'0102'", "'1.00'"};
    createTableWithColTypes(types, vals);

    runImport(getOutputArgv(true, extraArgs));

    Path outputFile = new Path(getTablePath(), "part-m-00000.avro");
    DataFileReader<GenericRecord> reader = read(outputFile);
    Schema schema = reader.getSchema();
    assertEquals(Schema.Type.RECORD, schema.getType());
    List<Field> fields = schema.getFields();
    assertEquals(types.length, fields.size());

    checkField(fields.get(0), "DATA_COL0", Schema.Type.BOOLEAN);
    checkField(fields.get(1), "DATA_COL1", Schema.Type.INT);
    checkField(fields.get(2), "DATA_COL2", Schema.Type.LONG);
    checkField(fields.get(3), "DATA_COL3", Schema.Type.FLOAT);
    checkField(fields.get(4), "DATA_COL4", Schema.Type.DOUBLE);
    checkField(fields.get(5), "DATA_COL5", Schema.Type.STRING);
    checkField(fields.get(6), "DATA_COL6", Schema.Type.BYTES);
    checkField(fields.get(7), "DATA_COL7", Schema.Type.STRING);

    GenericRecord record1 = reader.next();
    assertEquals("DATA_COL0", true, record1.get("DATA_COL0"));
    assertEquals("DATA_COL1", 100, record1.get("DATA_COL1"));
    assertEquals("DATA_COL2", 200L, record1.get("DATA_COL2"));
    assertEquals("DATA_COL3", 1.0f, record1.get("DATA_COL3"));
    assertEquals("DATA_COL4", 2.0, record1.get("DATA_COL4"));
    assertEquals("DATA_COL5", new Utf8("s"), record1.get("DATA_COL5"));
    Object object = record1.get("DATA_COL6");
    assertTrue(object instanceof ByteBuffer);
    ByteBuffer b = ((ByteBuffer) object);
    assertEquals((byte) 1, b.get(0));
    assertEquals((byte) 2, b.get(1));
    assertEquals("DATA_COL7", "1.00", record1.get("DATA_COL7").toString());

    if (codec != null) {
      assertEquals(codec, reader.getMetaString(DataFileConstants.CODEC));
    }

    checkSchemaFile(schema);
  }

  @Test
  public void testOverrideTypeMapping() throws IOException {
    String [] types = { "INT" };
    String [] vals = { "10" };
    createTableWithColTypes(types, vals);

    String [] extraArgs = { "--map-column-java", "DATA_COL0=String"};

    runImport(getOutputArgv(true, extraArgs));

    Path outputFile = new Path(getTablePath(), "part-m-00000.avro");
    DataFileReader<GenericRecord> reader = read(outputFile);
    Schema schema = reader.getSchema();
    assertEquals(Schema.Type.RECORD, schema.getType());
    List<Field> fields = schema.getFields();
    assertEquals(types.length, fields.size());

    checkField(fields.get(0), "DATA_COL0", Schema.Type.STRING);

    GenericRecord record1 = reader.next();
    assertEquals("DATA_COL0", new Utf8("10"), record1.get("DATA_COL0"));
  }

  @Test
  public void testFirstUnderscoreInColumnName() throws IOException {
    String [] names = { "_NAME" };
    String [] types = { "INT" };
    String [] vals = { "1987" };
    createTableWithColTypesAndNames(names, types, vals);

    runImport(getOutputArgv(true, null));

    Path outputFile = new Path(getTablePath(), "part-m-00000.avro");
    DataFileReader<GenericRecord> reader = read(outputFile);
    Schema schema = reader.getSchema();
    assertEquals(Schema.Type.RECORD, schema.getType());
    List<Field> fields = schema.getFields();
    assertEquals(types.length, fields.size());

    checkField(fields.get(0), "__NAME", Type.INT);

    GenericRecord record1 = reader.next();
    assertEquals("__NAME", 1987, record1.get("__NAME"));
  }

  @Test
  public void testNonstandardCharactersInColumnName() throws IOException {
    String [] names = { "avro\uC3A11" };
    String [] types = { "INT" };
    String [] vals = { "1987" };
    this.setCurTableName("Non_Std_Character_Test");
    createTableWithColTypesAndNames(names, types, vals);

    runImport(getOutputArgv(true, null));

    Path outputFile = new Path(getTablePath(), "part-m-00000.avro");
    DataFileReader<GenericRecord> reader = read(outputFile);
    Schema schema = reader.getSchema();
    assertEquals(Schema.Type.RECORD, schema.getType());
    List<Field> fields = schema.getFields();
    assertEquals(types.length, fields.size());

    checkField(fields.get(0), "AVRO\uC3A11", Type.INT);

    GenericRecord record1 = reader.next();
    assertEquals("AVRO\uC3A11", 1987, record1.get("AVRO\uC3A11"));
  }

  @Test
  public void testNonIdentCharactersInColumnName() throws IOException {
    String [] names = { "test_a-v+r/o" };
    String [] types = { "INT" };
    String [] vals = { "2015" };
    createTableWithColTypesAndNames(names, types, vals);

    runImport(getOutputArgv(true, null));

    Path outputFile = new Path(getTablePath(), "part-m-00000.avro");
    DataFileReader<GenericRecord> reader = read(outputFile);
    Schema schema = reader.getSchema();
    assertEquals(Schema.Type.RECORD, schema.getType());
    List<Field> fields = schema.getFields();
    assertEquals(types.length, fields.size());

    checkField(fields.get(0), "TEST_A_V_R_O", Type.INT);

    GenericRecord record1 = reader.next();
    assertEquals("TEST_A_V_R_O", 2015, record1.get("TEST_A_V_R_O"));
  }

  /*
   * Test Case For checking multiple columns having non standard characters in multiple columns
   */
  @Test
  public void testNonstandardCharactersInMultipleColumns() throws IOException {
    String[] names = { "id$1", "id1$" };
    String[] types = { "INT", "INT" };
    String[] vals = { "1987", "1988" };
    this.setCurTableName("Non_Std_Character_Test_For_Multiple_Columns");
    createTableWithColTypesAndNames(names, types, vals);

    runImport(getOutputArgv(true, null));

    Path outputFile = new Path(getTablePath(), "part-m-00000.avro");
    DataFileReader<GenericRecord> reader = read(outputFile);
    Schema schema = reader.getSchema();
    assertEquals(Schema.Type.RECORD, schema.getType());
    List<Field> fields = schema.getFields();
    assertEquals(types.length, fields.size());

    checkField(fields.get(0), "ID_1", Type.INT);

    GenericRecord record1 = reader.next();
    assertEquals("ID_1", 1987, record1.get("ID_1"));
    checkField(fields.get(1), "ID1_", Type.INT);
    assertEquals("ID1_", 1988, record1.get("ID1_"));
  }

  protected void checkField(Field field, String name, Type type) {
    assertEquals(name, field.name());
    assertEquals(Schema.Type.UNION, field.schema().getType());
    assertEquals(Schema.Type.NULL, field.schema().getTypes().get(0).getType());
    assertEquals(type, field.schema().getTypes().get(1).getType());
  }

  @Test
  public void testNullableAvroImport() throws IOException, SQLException {
    String [] types = { "INT" };
    String [] vals = { null };
    createTableWithColTypes(types, vals);

    runImport(getOutputArgv(true, null));

    Path outputFile = new Path(getTablePath(), "part-m-00000.avro");
    DataFileReader<GenericRecord> reader = read(outputFile);

    GenericRecord record1 = reader.next();
    assertNull(record1.get("DATA_COL0"));
  }

  @Test
  public void testSpecialCharactersInColumnMappingWithConvertion() throws IOException, SQLException {
    // escaping enabled by default
    String [] extraArgsEscapeColNamesWithMapping = { "--map-column-java",
        "INTFIELD1=String,DATA_#_COL0=String,DATA#COL1=String,DATA___COL2=String"};

    // disable escaping
    String [] extraArgsEscapingDisables = {"--escape-mapping-column-names", "false"};

    // escaping enabled but mapping not provided
    String [] extraArgsEscapingWithoutMapping = {};

    checkRecordWithExtraArgs(extraArgsEscapeColNamesWithMapping, "TABLE1");
    checkRecordWithExtraArgs(extraArgsEscapingDisables, "TABLE2");
    checkRecordWithExtraArgs(extraArgsEscapingWithoutMapping, "TABLE3");
  }

  private void checkRecordWithExtraArgs(String[] extraArgs, String tableName) throws IOException {
    String date = "2017-01-19";
    String timeStamp = "2017-01-19 14:47:57.112000";

    String [] names = {"INTFIELD1", "DATA_#_COL0", "DATA#COL1", "DATA___COL2"};
    String [] types = { "INT", "DATE", "TIMESTAMP", "DECIMAL(2,20)" };
    String [] vals = {"1", "{ts \'" + date + "\'}", "{ts \'" + timeStamp + "\'}", "2e20"};

    String [] checkNames =  {"INTFIELD1", "DATA___COL0", "DATA_COL1", "DATA___COL2"};

    setCurTableName(tableName);

    createTableWithColTypesAndNames(names, types, vals);
    runImport(getOutputArgv(true, extraArgs));

    Path outputFile = new Path(getTablePath(), "part-m-00000.avro");
    DataFileReader<GenericRecord> reader = read(outputFile);
    GenericRecord record = reader.next();

    for (String columnName : checkNames) {
      assertNotNull(record.get(columnName));
    }

    removeTableDir();
  }

  protected DataFileReader<GenericRecord> read(Path filename) throws IOException {
    Configuration conf = new Configuration();
    if (!BaseSqoopTestCase.isOnPhysicalCluster()) {
      conf.set(CommonArgs.FS_DEFAULT_NAME, CommonArgs.LOCAL_FS);
    }
    FsInput fsInput = new FsInput(filename, conf);
    DatumReader<GenericRecord> datumReader =
      new GenericDatumReader<GenericRecord>();
    return new DataFileReader<GenericRecord>(fsInput, datumReader);
  }

  protected void checkSchemaFile(final Schema schema) throws IOException {
    final File schemaFile = new File(schema.getName() + ".avsc");
    assertTrue(schemaFile.exists());
    assertEquals(schema, new Schema.Parser().parse(schemaFile));
  }
}
