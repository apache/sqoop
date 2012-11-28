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
package org.apache.sqoop.mapreduce.sqlserver;

import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.sqoop.mapreduce.DBWritable;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Input format specific for Microsoft SQL Server.
 */
public class SqlServerInputFormat<T extends DBWritable>
  extends DataDrivenDBInputFormat {

  /** {@inheritDoc} */
  @Override
  protected RecordReader<LongWritable, T> createDBRecordReader(
      DBInputSplit split, Configuration conf) throws IOException {

    DBConfiguration dbConf = getDBConf();
    @SuppressWarnings("unchecked")
    Class<T> inputClass = (Class<T>) (dbConf.getInputClass());

    try {
      // Use Microsoft SQL Server specific db reader
      return new SqlServerRecordReader<T>(split, inputClass,
          conf, getConnection(), dbConf, dbConf.getInputConditions(),
          dbConf.getInputFieldNames(), dbConf.getInputTableName());
    } catch (SQLException ex) {
      throw new IOException(ex);
    }
  }
}
