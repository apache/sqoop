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

package org.apache.hadoop.sqoop.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.sqoop.Sqoop;
import org.apache.hadoop.sqoop.mapreduce.AutoProgressMapper;
import org.apache.hadoop.sqoop.testutil.CommonArgs;
import org.apache.hadoop.sqoop.testutil.HsqldbTestServer;
import org.apache.hadoop.sqoop.testutil.ImportJobTestCase;
import org.apache.hadoop.util.ToolRunner;

/**
 * Test aspects of the DataDrivenImportJob class
 */
public class TestImportJob extends ImportJobTestCase {

  public void testFailedImportDueToIOException() throws IOException {
    // Make sure that if a MapReduce job to do the import fails due
    // to an IOException, we tell the user about it.

    // Create a table to attempt to import.
    createTableForColType("VARCHAR(32)", "'meep'");

    // Make the output dir exist so we know the job will fail via IOException.
    Path outputPath = new Path(new Path(getWarehouseDir()), getTableName());
    FileSystem fs = FileSystem.getLocal(new Configuration());
    fs.mkdirs(outputPath);

    assertTrue(fs.exists(outputPath));

    String [] argv = getArgv(true, new String [] { "DATA_COL0" });

    Sqoop importer = new Sqoop();
    try {
      ToolRunner.run(importer, argv);
      fail("Expected IOException running this job.");
    } catch (Exception e) {
      // In debug mode, IOException is wrapped in RuntimeException.
      LOG.info("Got exceptional return (expected: ok). msg is: " + e);
    }
  }

  // A mapper that is guaranteed to cause the task to fail.
  public static class NullDereferenceMapper
      extends AutoProgressMapper<LongWritable, DBWritable, Text, NullWritable> {

    public void map(LongWritable key, DBWritable val, Context c)
        throws IOException, InterruptedException {
      String s = null;
      s.length(); // This will throw a NullPointerException.
    }
  }

  public void testFailedImportDueToJobFail() throws IOException {
    // Test that if the job returns 'false' it still fails and informs
    // the user.

    // Create a table to attempt to import.
    createTableForColType("VARCHAR(32)", "'meep'");

    String [] argv = getArgv(true, new String [] { "DATA_COL0" });

    // Use dependency injection to specify a mapper that we know
    // will fail.
    Configuration conf = new Configuration();
    conf.setClass(DataDrivenImportJob.DATA_DRIVEN_MAPPER_KEY,
        NullDereferenceMapper.class,
        Mapper.class);

    Sqoop importer = new Sqoop(conf);
    try {
      ToolRunner.run(importer, argv);
      fail("Expected ImportException running this job.");
    } catch (Exception e) {
      // In debug mode, ImportException is wrapped in RuntimeException.
      LOG.info("Got exceptional return (expected: ok). msg is: " + e);
    }
  }

}

