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

package org.apache.sqoop.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import com.cloudera.sqoop.manager.MySQLUtils;
import com.cloudera.sqoop.mapreduce.MySQLExportMapper;

/**
 * mysqlimport-based exporter which accepts lines of text from files
 * in HDFS to emit to the database.
 */
public class MySQLTextExportMapper
    extends MySQLExportMapper<LongWritable, Text> {

  // End-of-record delimiter.
  private String recordEndStr;

  @Override
  protected void setup(Context context) {
    super.setup(context);

    char recordDelim = (char) conf.getInt(MySQLUtils.OUTPUT_RECORD_DELIM_KEY,
        (int) '\n');
    this.recordEndStr = "" + recordDelim;
  }

  /**
   * Export the table to MySQL by using mysqlimport to write the data to the
   * database.
   *
   * Expects one delimited text record as the 'val'; ignores the key.
   */
  @Override
  public void map(LongWritable key, Text val, Context context)
      throws IOException, InterruptedException {

    writeRecord(val.toString(), this.recordEndStr);

    // We don't emit anything to the OutputCollector because we wrote
    // straight to mysql. Send a progress indicator to prevent a timeout.
    context.progress();
  }

}
