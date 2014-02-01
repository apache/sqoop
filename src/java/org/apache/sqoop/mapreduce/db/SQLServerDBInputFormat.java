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

package org.apache.sqoop.mapreduce.db;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.sqoop.mapreduce.DBWritable;
import org.apache.sqoop.lib.SqoopRecord;

import com.cloudera.sqoop.mapreduce.db.DBConfiguration;

/**
 * A InputFormat that reads input data from a SQL table.
 * Operates like DataDrivenDBInputFormat, but attempts to recover from
 * connection failures based on set recovery options in job configurations
 */
public class SQLServerDBInputFormat<T extends SqoopRecord>
      extends DataDrivenDBInputFormat<T> implements Configurable  {

  private static final Log LOG =
      LogFactory.getLog(SQLServerDBInputFormat.class);

  public static final String IMPORT_FAILURE_HANDLER_CLASS =
      "sqoop.import.failure.handler.class";

  @Override
  /** {@inheritDoc} */
  protected RecordReader<LongWritable, T> createDBRecordReader(
      DBInputSplit split, Configuration conf) throws IOException {

    DBConfiguration dbConf = getDBConf();
    Class<T> inputClass = (Class<T>) (dbConf.getInputClass());
    String dbProductName = getDBProductName();
    LOG.debug("Creating db record reader for db product: " + dbProductName);

    try {
      return new SQLServerDBRecordReader<T>(split, inputClass,
          conf, getConnection(), dbConf, dbConf.getInputConditions(),
          dbConf.getInputFieldNames(), dbConf.getInputTableName(),
          dbProductName);
    } catch (SQLException ex) {
      throw new IOException(ex);
    }
  }

  /** Set Input for table. */
  public static void setInput(Job job,
      Class<? extends DBWritable> inputClass,
      String tableName, String conditions,
      String splitBy, String... fieldNames) {
    DataDrivenDBInputFormat.setInput(job, inputClass, tableName, conditions,
        splitBy, fieldNames);
    job.setInputFormatClass(SQLServerDBInputFormat.class);
  }

  /** Set Input for query. */
  public static void setInput(Job job,
      Class<? extends DBWritable> inputClass,
      String inputQuery, String inputBoundingQuery) {
    DataDrivenDBInputFormat.setInput(job, inputClass, inputQuery,
        inputBoundingQuery);
    job.setInputFormatClass(SQLServerDBInputFormat.class);
  }
}
