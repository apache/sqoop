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
package org.apache.sqoop.job;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;

import junit.framework.TestCase;

import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.job.JobEngine;
import org.apache.sqoop.job.etl.Context;
import org.apache.sqoop.job.etl.EtlOptions;
import org.apache.sqoop.job.etl.Exporter;
import org.apache.sqoop.job.etl.Extractor;
import org.apache.sqoop.job.etl.Importer;
import org.apache.sqoop.job.etl.Initializer;
import org.apache.sqoop.job.etl.MutableContext;
import org.apache.sqoop.job.etl.Options;
import org.apache.sqoop.job.etl.Partition;
import org.apache.sqoop.job.etl.Partitioner;
import org.apache.sqoop.job.io.Data;
import org.apache.sqoop.job.io.DataWriter;
import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MJob.Type;
import org.apache.sqoop.model.MJobForms;
import org.apache.sqoop.validation.Validator;
import org.junit.Test;

public class TestJobEngine extends TestCase {

  private static final String DATA_DIR = TestJobEngine.class.getSimpleName();
  private static final String WAREHOUSE_ROOT = "/tmp/sqoop/warehouse/";

  private static final String OUTPUT_DIR = WAREHOUSE_ROOT + DATA_DIR;
  private static final String OUTPUT_FILE = "part-r-00000";
  private static final int START_PARTITION = 1;
  private static final int NUMBER_OF_PARTITIONS = 9;
  private static final int NUMBER_OF_ROWS_PER_PARTITION = 10;

  @Test
  public void testImport() throws Exception {
    FileUtils.delete(OUTPUT_DIR);

    DummyConnector connector = new DummyConnector();
    EtlOptions options = new EtlOptions(connector);

    JobEngine engine = new JobEngine();
    engine.run(options);

    String fileName = OUTPUT_DIR + "/" + OUTPUT_FILE;
    InputStream filestream = FileUtils.open(fileName);
    BufferedReader filereader = new BufferedReader(new InputStreamReader(
        filestream, Data.CHARSET_NAME));
    verifyOutput(filereader);
  }

  private void verifyOutput(BufferedReader reader)
      throws IOException {
    String line = null;
    int index = START_PARTITION*NUMBER_OF_ROWS_PER_PARTITION;
    Data expected = new Data();
    while ((line = reader.readLine()) != null){
      expected.setContent(new Object[] {
          new Integer(index),
          new Double(index),
          String.valueOf(index) },
          Data.ARRAY_RECORD);
      index++;

      assertEquals(expected.toString(), line);
    }
    reader.close();

    assertEquals(NUMBER_OF_PARTITIONS*NUMBER_OF_ROWS_PER_PARTITION,
        index-START_PARTITION*NUMBER_OF_ROWS_PER_PARTITION);
  }

  public class DummyConnector implements SqoopConnector {

    @Override
    public Importer getImporter() {
      return new Importer(
          DummyImportInitializer.class,
          DummyImportPartitioner.class,
          DummyImportExtractor.class,
          null);
    }

    @Override
    public Exporter getExporter() {
      fail("This method should not be invoked.");
      return null;
    }

    @Override
    public ResourceBundle getBundle(Locale locale) {
      fail("This method should not be invoked.");
      return null;
    }

    @Override
    public Validator getValidator() {
      fail("This method should not be invoked.");
      return null;
    }

    @Override
    public Class getConnectionConfigurationClass() {
      fail("This method should not be invoked.");
      return null;
    }

    @Override
    public Class getJobConfigurationClass(Type jobType) {
      fail("This method should not be invoked.");
      return null;
    }
  }

  public static class DummyImportInitializer extends Initializer {
    @Override
    public void run(MutableContext context, Options options) {
      context.setString(Constants.JOB_ETL_OUTPUT_DIRECTORY, OUTPUT_DIR);
    }
  }

  public static class DummyImportPartition extends Partition {
    private int id;

    public void setId(int id) {
      this.id = id;
    }

    public int getId() {
      return id;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      id = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(id);
    }
  }

  public static class DummyImportPartitioner extends Partitioner {
    @Override
    public List<Partition> run(Context context) {
      List<Partition> partitions = new LinkedList<Partition>();
      for (int id = START_PARTITION; id <= NUMBER_OF_PARTITIONS; id++) {
        DummyImportPartition partition = new DummyImportPartition();
        partition.setId(id);
        partitions.add(partition);
      }
      return partitions;
    }
  }

  public static class DummyImportExtractor extends Extractor {
    @Override
    public void run(Context context, Partition partition, DataWriter writer) {
      int id = ((DummyImportPartition)partition).getId();
      for (int row = 0; row < NUMBER_OF_ROWS_PER_PARTITION; row++) {
        writer.writeArrayRecord(new Object[] {
            new Integer(id*NUMBER_OF_ROWS_PER_PARTITION+row),
            new Double(id*NUMBER_OF_ROWS_PER_PARTITION+row),
            String.valueOf(id*NUMBER_OF_ROWS_PER_PARTITION+row)});
      }
    }
  }

}
