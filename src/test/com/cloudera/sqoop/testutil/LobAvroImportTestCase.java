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

package com.cloudera.sqoop.testutil;

import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.sqoop.io.CodecMap;
import org.apache.sqoop.lib.BlobRef;

/**
 * Tests BLOB/CLOB import for Avro.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class LobAvroImportTestCase extends ImportJobTestCase {

  private Log log;

  public LobAvroImportTestCase() {
    this.log = LogFactory.getLog(LobAvroImportTestCase.class.getName());
  }

  /**
   * @return the Log object to use for reporting during this test
   */
  protected abstract Log getLogger();

  /**
   * @return a "friendly" name for the database. e.g "mysql" or "oracle".
   */
  protected abstract String getDbFriendlyName();

  @Override
  protected String getTablePrefix() {
    return "LOB_" + getDbFriendlyName().toUpperCase() + "_";
  }

  @Override
  protected boolean useHsqldbTestServer() {
    // Hsqldb does not support BLOB/CLOB
    return false;
  }

  @Override
  public void tearDown() {
    try {
      // Clean up the database on our way out.
      dropTableIfExists(getTableName());
    } catch (SQLException e) {
      log.warn("Error trying to drop table '" + getTableName()
          + "' on tearDown: " + e);
    }
    super.tearDown();
  }

  protected String [] getArgv(String ... additionalArgs) {
    // Import every column of the table
    String [] colNames = getColNames();
    String splitByCol = colNames[0];
    String columnsString = "";
    for (String col : colNames) {
      columnsString += col + ",";
    }

    ArrayList<String> args = new ArrayList<String>();

    CommonArgs.addHadoopFlags(args);

    args.add("--table");
    args.add(getTableName());
    args.add("--columns");
    args.add(columnsString);
    args.add("--split-by");
    args.add(splitByCol);
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(getConnectString());
    args.add("--as-avrodatafile");
    args.add("--num-mappers");
    args.add("2");

    for (String arg : additionalArgs) {
      args.add(arg);
    }

    return args.toArray(new String[0]);
  }

  protected String getBlobType() {
    return "BLOB";
  }

  protected String getBlobInsertStr(String blobData) {
    return "'" + blobData + "'";
  }

  /**
   * Return the current table number as a string. In test, table number is used
   * to name .lob files.
   * @return current table number.
   */
  private String getTableNum() {
    return getTableName().substring(getTablePrefix().length());
  }

  /**
   * Return an instance of DataFileReader for the given filename.
   * @param filename path that we're opening a reader for.
   * @return instance of DataFileReader.
   * @throws IOException
   */
  private DataFileReader<GenericRecord> read(Path filename)
      throws IOException {
    Configuration conf = getConf();
    if (!BaseSqoopTestCase.isOnPhysicalCluster()) {
      conf.set(CommonArgs.FS_DEFAULT_NAME, CommonArgs.LOCAL_FS);
    }
    FsInput fsInput = new FsInput(filename, conf);
    DatumReader<GenericRecord> datumReader =
      new GenericDatumReader<GenericRecord>();
    return new DataFileReader<GenericRecord>(fsInput, datumReader);
  }

  /** Import blob data that is smaller than inline lob limit. Blob data
   * should be saved as Avro bytes.
   * @throws IOException
   * @throws SQLException
   */
  public void testBlobAvroImportInline() throws IOException, SQLException {
    String [] types = { getBlobType() };
    String expectedVal = "This is short BLOB data";
    String [] vals = { getBlobInsertStr(expectedVal) };

    createTableWithColTypes(types, vals);

    runImport(getArgv());

    Path outputFile = new Path(getTablePath(), "part-m-00000.avro");
    DataFileReader<GenericRecord> reader = read(outputFile);
    GenericRecord record = reader.next();

    // Verify that blob data is imported as Avro bytes.
    ByteBuffer buf = (ByteBuffer) record.get(getColName(0));
    String returnVal = new String(buf.array());

    assertEquals(getColName(0), expectedVal, returnVal);
  }

  /**
   * Import blob data that is larger than inline lob limit. The reference file
   * should be saved as Avro bytes. Blob data should be saved in LOB file
   * format.
   * @throws IOException
   * @throws SQLException
   */
  public void testBlobAvroImportExternal() throws IOException, SQLException {
    String [] types = { getBlobType() };
    String data = "This is short BLOB data";
    String [] vals = { getBlobInsertStr(data) };

    createTableWithColTypes(types, vals);

    // Set inline lob limit to a small value so that blob data will be
    // written to an external file.
    runImport(getArgv("--inline-lob-limit", "1"));

    Path outputFile = new Path(getTablePath(), "part-m-00000.avro");
    DataFileReader<GenericRecord> reader = read(outputFile);
    GenericRecord record = reader.next();

    // Verify that the reference file is written in Avro bytes.
    ByteBuffer buf = (ByteBuffer) record.get(getColName(0));
    String returnVal = new String(buf.array());
    String expectedStart = "externalLob(lf,_lob/large_obj";
    String expectedEnd = getTableNum() + "_m_0000000.lob,68,"
      + data.length() + ")";

    assertNotNull(returnVal);
    assertTrue("ExpectedStart: " + expectedStart + ", value: " + returnVal, returnVal.startsWith(expectedStart));
    assertTrue("ExpectedEnd: " + expectedEnd + ", value: " + returnVal, returnVal.endsWith(expectedEnd));

    // Verify that blob data stored in the external lob file is correct.
    BlobRef br = BlobRef.parse(returnVal);
    Path lobFileDir = new Path(getWarehouseDir(), getTableName());
    InputStream in = br.getDataStream(getConf(), lobFileDir);

    byte [] bufArray = new byte[data.length()];
    int chars = in.read(bufArray);
    in.close();

    assertEquals(chars, data.length());

    returnVal = new String(bufArray);
    String expectedVal = data;

    assertEquals(getColName(0), returnVal, expectedVal);
  }

  /**
   * Import blob data that is smaller than inline lob limit and compress with
   * deflate codec. Blob data should be encoded and saved as Avro bytes.
   * @throws IOException
   * @throws SQLException
   */
  public void testBlobCompressedAvroImportInline()
      throws IOException, SQLException {
    String [] types = { getBlobType() };
    String expectedVal = "This is short BLOB data";
    String [] vals = { getBlobInsertStr(expectedVal) };

    createTableWithColTypes(types, vals);

    runImport(getArgv("--compression-codec", CodecMap.DEFLATE));

    Path outputFile = new Path(getTablePath(), "part-m-00000.avro");
    DataFileReader<GenericRecord> reader = read(outputFile);
    GenericRecord record = reader.next();

    // Verify that the data block of the Avro file is compressed with deflate
    // codec.
    assertEquals(CodecMap.DEFLATE,
        reader.getMetaString(DataFileConstants.CODEC));

    // Verify that all columns are imported correctly.
    ByteBuffer buf = (ByteBuffer) record.get(getColName(0));
    String returnVal = new String(buf.array());

    assertEquals(getColName(0), expectedVal, returnVal);
  }

  /**
   * Import blob data that is larger than inline lob limit and compress with
   * deflate codec. The reference file should be encoded and saved as Avro
   * bytes. Blob data should be saved in LOB file format without compression.
   * @throws IOException
   * @throws SQLException
   */
  public void testBlobCompressedAvroImportExternal()
      throws IOException, SQLException {
    String [] types = { getBlobType() };
    String data = "This is short BLOB data";
    String [] vals = { getBlobInsertStr(data) };

    createTableWithColTypes(types, vals);

    // Set inline lob limit to a small value so that blob data will be
    // written to an external file.
    runImport(getArgv(
        "--inline-lob-limit", "1", "--compression-codec", CodecMap.DEFLATE));

    Path outputFile = new Path(getTablePath(), "part-m-00000.avro");
    DataFileReader<GenericRecord> reader = read(outputFile);
    GenericRecord record = reader.next();

    // Verify that the data block of the Avro file is compressed with deflate
    // codec.
    assertEquals(CodecMap.DEFLATE,
        reader.getMetaString(DataFileConstants.CODEC));

    // Verify that the reference file is written in Avro bytes.
    ByteBuffer buf = (ByteBuffer) record.get(getColName(0));
    String returnVal = new String(buf.array());
    String expectedStart = "externalLob(lf,_lob/large_obj";
    String expectedEnd = getTableNum() + "_m_0000000.lob,68,"
      + data.length() + ")";

    assertNotNull(returnVal);
    assertTrue("ExpectedStart: " + expectedStart + ", value: " + returnVal, returnVal.startsWith(expectedStart));
    assertTrue("ExpectedEnd: " + expectedEnd + ", value: " + returnVal, returnVal.endsWith(expectedEnd));

    // Verify that blob data stored in the external lob file is correct.
    BlobRef br = BlobRef.parse(returnVal);
    Path lobFileDir = new Path(getWarehouseDir(), getTableName());
    InputStream in = br.getDataStream(getConf(), lobFileDir);

    byte [] bufArray = new byte[data.length()];
    int chars = in.read(bufArray);
    in.close();

    assertEquals(chars, data.length());

    returnVal = new String(bufArray);
    String expectedVal = data;

    assertEquals(getColName(0), returnVal, expectedVal);
  }

  /**
   * Import multiple columns of blob data. Blob data should be saved as Avro
   * bytes.
   * @throws IOException
   * @throws SQLException
   */
  public void testBlobAvroImportMultiCols() throws IOException, SQLException {
    String [] types = { getBlobType(), getBlobType(), getBlobType(), };
    String expectedVal1 = "This is short BLOB data1";
    String expectedVal2 = "This is short BLOB data2";
    String expectedVal3 = "This is short BLOB data3";
    String [] vals = { getBlobInsertStr(expectedVal1),
                       getBlobInsertStr(expectedVal2),
                       getBlobInsertStr(expectedVal3), };

    createTableWithColTypes(types, vals);

    runImport(getArgv());

    Path outputFile = new Path(getTablePath(), "part-m-00000.avro");
    DataFileReader<GenericRecord> reader = read(outputFile);
    GenericRecord record = reader.next();

    // Verify that all columns are imported correctly.
    ByteBuffer buf = (ByteBuffer) record.get(getColName(0));
    String returnVal = new String(buf.array());

    assertEquals(getColName(0), expectedVal1, returnVal);

    buf = (ByteBuffer) record.get(getColName(1));
    returnVal = new String(buf.array());

    assertEquals(getColName(1), expectedVal2, returnVal);

    buf = (ByteBuffer) record.get(getColName(2));
    returnVal = new String(buf.array());

    assertEquals(getColName(2), expectedVal3, returnVal);
  }

  public void testClobAvroImportInline() throws IOException, SQLException {
    // TODO: add tests for CLOB support for Avro import
  }

  public void testClobAvroImportExternal() throws IOException, SQLException {
    // TODO: add tests for CLOB support for Avro import
  }

  public void testClobCompressedAvroImportInline()
      throws IOException, SQLException {
    // TODO: add tests for CLOB support for Avro import
  }

  public void testClobCompressedAvroImportExternal()
      throws IOException, SQLException {
    // TODO: add tests for CLOB support for Avro import
  }

  public void testClobAvroImportMultiCols() throws IOException, SQLException {
    // TODO: add tests for CLOB support for Avro import
  }
}
