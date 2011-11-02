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

import java.io.IOException;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import com.cloudera.sqoop.lib.SqoopRecord;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Test harness mapper. Instantiate the user's specific type, explicitly
 * set the value of a field with setField(), and read the field value
 * back via the field map. Throw an IOException if it doesn't get set
 * correctly.
 */
public class ExplicitSetMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, NullWritable> {

  public static final Log LOG = LogFactory.getLog(
      ExplicitSetMapper.class.getName());

  public static final String USER_TYPE_NAME_KEY = "sqoop.user.class";
  public static final String SET_COL_KEY = "sqoop.explicit.set.col";
  public static final String SET_VAL_KEY = "sqoop.explicit.set.val";

  private SqoopRecord userRecord;
  private String setCol;
  private String setVal;

  public void configure(JobConf job) {
    String userTypeName = job.get(USER_TYPE_NAME_KEY);
    if (null == userTypeName) {
      throw new RuntimeException("Unconfigured parameter: "
          + USER_TYPE_NAME_KEY);
    }

    setCol = job.get(SET_COL_KEY);
    setVal = job.get(SET_VAL_KEY);

    LOG.info("User type name set to " + userTypeName);
    LOG.info("Will try to set col " + setCol + " to " + setVal);

    this.userRecord = null;

    try {
      Configuration conf = new Configuration();
      Class userClass = Class.forName(userTypeName, true,
          Thread.currentThread().getContextClassLoader());
      this.userRecord =
          (SqoopRecord) ReflectionUtils.newInstance(userClass, conf);
    } catch (ClassNotFoundException cnfe) {
      // handled by the next block.
      LOG.error("ClassNotFound exception: " + cnfe.toString());
    } catch (Exception e) {
      LOG.error("Got an exception reflecting user class: " + e.toString());
    }

    if (null == this.userRecord) {
      LOG.error("Could not instantiate user record of type " + userTypeName);
      throw new RuntimeException("Could not instantiate user record of type "
          + userTypeName);
    }
  }

  public void map(LongWritable key, Text val,
      OutputCollector<Text, NullWritable> out, Reporter r) throws IOException {

    // Try to set the field.
    userRecord.setField(setCol, setVal);
    Map<String, Object> fieldVals = userRecord.getFieldMap();
    if (!fieldVals.get(setCol).equals(setVal)) {
      throw new IOException("Could not set column value! Got back "
          + fieldVals.get(setCol));
    } else {
      LOG.info("Correctly changed value for col " + setCol + " to " + setVal);
    }
  }
}

