/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.cloudera.sqoop.testutil.BaseSqoopTestCase;
import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.HsqldbTestServer;
import com.cloudera.sqoop.testutil.ImportJobTestCase;

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
  protected String[] getOutputArgv(boolean includeHadoopFlags) {
    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
    }

    args.add("--table");
    args.add(HsqldbTestServer.getTableName());
    args.add("--connect");
    args.add(HsqldbTestServer.getUrl());
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--split-by");
    args.add("INTFIELD1");
    args.add("--as-avrodatafile");

    return args.toArray(new String[0]);
  }

  // this test just uses the two int table.
  protected String getTableName() {
    return HsqldbTestServer.getTableName();
  }

  public void testAvroImport() throws IOException {

    runImport(getOutputArgv(true));

    Path outputFile = new Path(getTablePath(), "part-m-00000.avro");
    DataFileReader<GenericRecord> reader = read(outputFile);
    Schema schema = reader.getSchema();
    assertEquals(Schema.Type.RECORD, schema.getType());
    List<Field> fields = schema.getFields();
    assertEquals(2, fields.size());

    assertEquals("INTFIELD1", fields.get(0).name());
    assertEquals(Schema.Type.UNION, fields.get(0).schema().getType());
    assertEquals(Schema.Type.INT, fields.get(0).schema().getTypes().get(0).getType());
    assertEquals(Schema.Type.NULL, fields.get(0).schema().getTypes().get(1).getType());

    assertEquals("INTFIELD2", fields.get(1).name());
    assertEquals(Schema.Type.UNION, fields.get(1).schema().getType());
    assertEquals(Schema.Type.INT, fields.get(1).schema().getTypes().get(0).getType());
    assertEquals(Schema.Type.NULL, fields.get(1).schema().getTypes().get(1).getType());

    GenericRecord record1 = reader.next();
    assertEquals(1, record1.get("INTFIELD1"));
    assertEquals(8, record1.get("INTFIELD2"));
  }

  public void testNullableAvroImport() throws IOException, SQLException {
    addNullRecord(); // Add a pair of NULL values to twointtable.
    runImport(getOutputArgv(true));

    Path outputFile = new Path(getTablePath(), "part-m-00000.avro");
    DataFileReader<GenericRecord> reader = read(outputFile);
    boolean foundNullRecord = false;

    // Iterate thru the records in the output file til we find one that
    // matches (NULL, NULL).
    for (GenericRecord record : reader) {
      LOG.debug("Input record: " + record);
      if (record.get("INTFIELD1") == null && record.get("INTFIELD2") == null) {
        LOG.debug("Got null record");
        foundNullRecord = true;
      }
    }

    assertTrue(foundNullRecord);
  }

  /**
   * Add a record to the TWOINTTABLE that contains (NULL, NULL).
   *
   * @throws SQLException if there's a problem doing the INSERT statement.
   */
  private void addNullRecord() throws SQLException {
    Connection connection = null;
    Statement st = null;
    try {
      connection = this.getManager().getConnection();
      st = connection.createStatement();
      st.executeUpdate("INSERT INTO " + getTableName()
          + " VALUES(NULL, NULL)");

      connection.commit();
    } finally {
      if (null != st) {
        st.close();
      }

      if (null != connection) {
        connection.close();
      }
    }
  }

  private DataFileReader<GenericRecord> read(Path filename) throws IOException {
    Configuration conf = new Configuration();
    if (!BaseSqoopTestCase.isOnPhysicalCluster()) {
      conf.set(CommonArgs.FS_DEFAULT_NAME, CommonArgs.LOCAL_FS);
    }
    FsInput fsInput = new FsInput(filename, conf);
    DatumReader<GenericRecord> datumReader =
      new GenericDatumReader<GenericRecord>();
    return new DataFileReader<GenericRecord>(fsInput, datumReader);
  }

}
