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
package org.apache.sqoop.mapreduce.db.netezza;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.sqoop.config.ConfigurationHelper;
import org.apache.sqoop.lib.RecordParser;
import org.apache.sqoop.lib.SqoopRecord;
import org.apache.sqoop.mapreduce.hcat.SqoopHCatImportHelper;

/**
 * Netezza import mapper using external tables for HCat integration.
 */
public class NetezzaExternalTableHCatImportMapper
  extends NetezzaExternalTableImportMapper<NullWritable, HCatRecord> {
  private SqoopHCatImportHelper helper;
  private SqoopRecord sqoopRecord;

  @Override
  protected void setup(Context context)
    throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    helper = new SqoopHCatImportHelper(conf);
    String recordClassName = conf.get(ConfigurationHelper
      .getDbInputClassProperty());
    if (null == recordClassName) {
      throw new IOException("DB Input class name is not set!");
    }
    try {
      Class<?> cls = Class.forName(recordClassName, true,
        Thread.currentThread().getContextClassLoader());
      sqoopRecord = (SqoopRecord) ReflectionUtils.newInstance(cls, conf);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException(cnfe);
    }

    if (null == sqoopRecord) {
      throw new IOException("Could not instantiate object of type "
        + recordClassName);
    }
  }

  @Override
  protected void writeRecord(Text text, Context context)
    throws IOException, InterruptedException {
    try {
      sqoopRecord.parse(text);
      context.write(NullWritable.get(),
        helper.convertToHCatRecord(sqoopRecord));
    } catch (RecordParser.ParseError pe) {
      throw new IOException("Exception parsing netezza import record", pe);
    }

  }
}
