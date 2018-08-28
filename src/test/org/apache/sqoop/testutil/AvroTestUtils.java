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

package org.apache.sqoop.testutil;

import org.apache.avro.Conversions;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

public class AvroTestUtils {

  private static final String OUTPUT_FILE_NAME = "part-m-00000.avro";

  public static final Log LOG = LogFactory.getLog(
          AvroTestUtils.class.getName());

  public static List<String[]> getInputData() {
    List<String[]> data = new ArrayList<>();
    data.add(new String[]{"1", "'Aaron'", "1000000.05", "'engineering'"});
    data.add(new String[]{"2", "'Bob'", "400.10", "'sales'"});
    data.add(new String[]{"3", "'Fred'", "15.23", "'marketing'"});
    return data;
  }

  public static String[] getExpectedResults() {
    return new String[] {
        "{\"ID\": 1, \"NAME\": \"Aaron\", \"SALARY\": 1000000.05000, \"DEPT\": \"engineering\"}",
        "{\"ID\": 2, \"NAME\": \"Bob\", \"SALARY\": 400.10000, \"DEPT\": \"sales\"}",
        "{\"ID\": 3, \"NAME\": \"Fred\", \"SALARY\": 15.23000, \"DEPT\": \"marketing\"}"
    };
  }

  public static ArgumentArrayBuilder getBuilderForAvroPaddingTest(BaseSqoopTestCase testCase) {
    ArgumentArrayBuilder builder = new ArgumentArrayBuilder();
    return builder.withCommonHadoopFlags(true)
        .withProperty("sqoop.avro.logical_types.decimal.enable", "true")
        .withOption("as-avrodatafile")
        .withOption("warehouse-dir", testCase.getWarehouseDir())
        .withOption("num-mappers", "1")
        .withOption("table", testCase.getTableName());
  }

  public static void registerDecimalConversionUsageForVerification() {
    GenericData.get().addLogicalTypeConversion(new Conversions.DecimalConversion());
  }

  public static void verify(String[] expectedResults, Configuration conf, Path tablePath) {
    Path outputFile = new Path(tablePath, OUTPUT_FILE_NAME);
    readAndVerify(expectedResults, conf, outputFile);
  }

  public static void verify(String[] expectedResults, Configuration conf, Path tablePath, String outputFileName) {
    Path outputFile = new Path(tablePath, outputFileName + ".avro");
    readAndVerify(expectedResults, conf, outputFile);
  }

  private static void readAndVerify(String[] expectedResults, Configuration conf, Path outputFile) {
    try (DataFileReader<GenericRecord> reader = read(outputFile, conf)) {
      GenericRecord record;
      if (!reader.hasNext() && expectedResults != null && expectedResults.length > 0) {
        fail("Empty file was not expected");
      }
      int i = 0;
      while (reader.hasNext()){
        record = reader.next();
        assertEquals(expectedResults[i++], record.toString());
      }
      if (expectedResults != null && expectedResults.length > i) {
        fail("More output data was expected");
      }
    }
    catch (IOException ioe) {
      LOG.error("Issue with verifying the output", ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Return an instance of DataFileReader for the given filename.
   * @param filename path that we're opening a reader for.
   * @param conf
   * @return instance of DataFileReader.
   * @throws IOException
   */
  public static DataFileReader<GenericRecord> read(Path filename, Configuration conf) throws IOException {
    if (!BaseSqoopTestCase.isOnPhysicalCluster()) {
      conf.set(CommonArgs.FS_DEFAULT_NAME, CommonArgs.LOCAL_FS);
    }
    FsInput fsInput = new FsInput(filename, conf);
    DatumReader<GenericRecord> datumReader =  new GenericDatumReader<>();
    return new DataFileReader<>(fsInput, datumReader);
  }
}
