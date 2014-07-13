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

package com.cloudera.sqoop.mapreduce;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import com.cloudera.sqoop.ConnFactory;
import com.cloudera.sqoop.Sqoop;
import com.cloudera.sqoop.manager.ManagerFactory;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import com.cloudera.sqoop.testutil.InjectableManagerFactory;
import com.cloudera.sqoop.testutil.InjectableConnManager;
import com.cloudera.sqoop.tool.ImportTool;
import org.apache.hadoop.util.StringUtils;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.util.ClassLoaderStack;

/**
 * Test aspects of the DataDrivenImportJob class' failure reporting.
 *
 * These tests have strange error checking because they have different correct
 * exit conditions when run in "debug mode" when run by ('ant test') and when
 * run within eclipse.  Debug mode is entered by setting system property
 * SQOOP_RETHROW_PROPERTY = "sqoop.throwOnError".
 */
public class TestImportJob extends ImportJobTestCase {
  public void testFailedImportDueToIOException() throws IOException {
    // Make sure that if a MapReduce job to do the import fails due
    // to an IOException, we tell the user about it.

    // Create a table to attempt to import.
    createTableForColType("VARCHAR(32)", "'meep'");

    Configuration conf = new Configuration();

    LogFactory.getLog(getClass()).info(
            " getWarehouseDir() " + getWarehouseDir());

    // Make the output dir exist so we know the job will fail via IOException.
    Path outputPath = new Path(new Path(getWarehouseDir()), getTableName());
    FileSystem fs = FileSystem.getLocal(conf);
    fs.mkdirs(outputPath);

    assertTrue(fs.exists(outputPath));

    String[] argv = getArgv(true, new String[] { "DATA_COL0" }, conf);

    Sqoop importer = new Sqoop(new ImportTool());
    try {
      int ret = Sqoop.runSqoop(importer, argv);
      assertTrue("Expected ImportException running this job.", 1==ret);
    } catch (Exception e) {
      // In debug mode, IOException is wrapped in RuntimeException.
      LOG.info("Got exceptional return (expected: ok). msg is: " + e);
    }
  }

  /** A mapper that is guaranteed to cause the task to fail. */
  public static class NullDereferenceMapper extends
      AutoProgressMapper<Object, Object, Text, NullWritable> {

    public void map(Object key, Object val, Context c) throws IOException,
        InterruptedException {
      String s = null;
      s.length(); // This will throw a NullPointerException.
    }
  }

  /** Run a "job" that just delivers a record to the mapper. */
  public static class DummyImportJob extends ImportJobBase {
    @Override
    public void configureInputFormat(Job job, String tableName,
        String tableClassName, String splitByCol)
        throws ClassNotFoundException, IOException {

      // Write a line of text into a file so that we can get
      // a record to the map task.
      Path dir = new Path(this.options.getTempDir());
      Path p = new Path(dir, "sqoop-dummy-import-job-file.txt");
      FileSystem fs = FileSystem.getLocal(this.options.getConf());
      if (fs.exists(p)) {
        boolean result = fs.delete(p, false);
        assertTrue("Couldn't delete temp file!", result);
      }

      BufferedWriter w = new BufferedWriter(
          new OutputStreamWriter(fs.create(p)));
      w.append("This is a line!");
      w.close();

      FileInputFormat.addInputPath(job, p);

      // And set the InputFormat itself.
      super.configureInputFormat(job, tableName, tableClassName, splitByCol);
    }
  }

  public void testFailedImportDueToJobFail() throws IOException {
    // Test that if the job returns 'false' it still fails and informs
    // the user.

    // Create a table to attempt to import.
    createTableForColType("VARCHAR(32)", "'meep2'");

    Configuration conf = new Configuration();

    // Use the dependency-injection manager.
    conf.setClass(ConnFactory.FACTORY_CLASS_NAMES_KEY,
        InjectableManagerFactory.class, ManagerFactory.class);

    String[] argv = getArgv(true, new String[] { "DATA_COL0" }, conf);

    // Use dependency injection to specify a mapper that we know
    // will fail.
    conf.setClass(InjectableConnManager.MAPPER_KEY,
        NullDereferenceMapper.class, Mapper.class);

    conf.setClass(InjectableConnManager.IMPORT_JOB_KEY, DummyImportJob.class,
        ImportJobBase.class);

    Sqoop importer = new Sqoop(new ImportTool(), conf);
    try {
      int ret = Sqoop.runSqoop(importer, argv);
      assertTrue("Expected ImportException running this job.", 1==ret);
    } catch (Exception e) {
      // In debug mode, ImportException is wrapped in RuntimeException.
      LOG.info("Got exceptional return (expected: ok). msg is: " + e);
    }
  }

  public void testFailedNoColumns() throws IOException {
    // Make sure that if a MapReduce job to do the import fails due
    // to an IOException, we tell the user about it.

    // Create a table to attempt to import.
    createTableForColType("VARCHAR(32)", "'meep'");

    Configuration conf = new Configuration();

    // Make the output dir exist so we know the job will fail via IOException.
    Path outputPath = new Path(new Path(getWarehouseDir()), getTableName());
    FileSystem fs = FileSystem.getLocal(conf);
    fs.mkdirs(outputPath);
    assertTrue(fs.exists(outputPath));

    String [] argv = getArgv(true, new String [] { "" }, conf);

    Sqoop importer = new Sqoop(new ImportTool());
    try {
      int ret = Sqoop.runSqoop(importer, argv);
      assertTrue("Expected job to fail due to no colnames.", 1==ret);
    } catch (Exception e) {
      // In debug mode, IOException is wrapped in RuntimeException.
      LOG.info("Got exceptional return (expected: ok). msg is: " + e);
    }
  }

  public void testFailedIllegalColumns() throws IOException {
    // Make sure that if a MapReduce job to do the import fails due
    // to an IOException, we tell the user about it.

    // Create a table to attempt to import.
    createTableForColType("VARCHAR(32)", "'meep'");

    Configuration conf = new Configuration();

    // Make the output dir exist so we know the job will fail via IOException.
    Path outputPath = new Path(new Path(getWarehouseDir()), getTableName());
    FileSystem fs = FileSystem.getLocal(conf);
    fs.mkdirs(outputPath);

    assertTrue(fs.exists(outputPath));

    // DATA_COL0 ok, by zyzzyva not good
    String [] argv = getArgv(true, new String [] { "DATA_COL0", "zyzzyva" },
        conf);

    Sqoop importer = new Sqoop(new ImportTool());
    try {
      int ret = Sqoop.runSqoop(importer, argv);
      assertTrue("Expected job to fail due bad colname.", 1==ret);
    } catch (Exception e) {
      // In debug mode, IOException is wrapped in RuntimeException.
      LOG.info("Got exceptional return (expected: ok). msg is: " + e);
    }
  }

  public void testDuplicateColumns() throws IOException {
    // Make sure that if a MapReduce job to do the import fails due
    // to an IOException, we tell the user about it.

    // Create a table to attempt to import.
    createTableForColType("VARCHAR(32)", "'meep'");

    Configuration conf = new Configuration();

    // Make the output dir exist so we know the job will fail via IOException.
    Path outputPath = new Path(new Path(getWarehouseDir()), getTableName());
    FileSystem fs = FileSystem.getLocal(conf);
    fs.mkdirs(outputPath);
    assertTrue(fs.exists(outputPath));

    String[] argv = getArgv(true, new String[] { "DATA_COL0,DATA_COL0" }, conf);

    Sqoop importer = new Sqoop(new ImportTool());
    try {
      int ret = Sqoop.runSqoop(importer, argv);
      assertTrue("Expected job to fail!", 1 == ret);
    } catch (Exception e) {
      // In debug mode, ImportException is wrapped in RuntimeException.
      LOG.info("Got exceptional return (expected: ok). msg is: " + e);
    }
  }

  // helper method to get contents of a given dir containing sequence files
  private String[] getContent(Configuration conf, Path path) throws Exception {
    ClassLoader prevClassLoader = ClassLoaderStack.addJarFile(
        new Path(new Path(new SqoopOptions().getJarOutputDir()), getTableName() + ".jar").toString(),
        getTableName());

    FileSystem fs = FileSystem.getLocal(conf);
    FileStatus[] stats = fs.listStatus(path);
    Path[] paths = new Path[stats.length];
    for (int i = 0; i < stats.length; i++) {
      paths[i] = stats[i].getPath();
    }

    // Read all the files adding the value lines to the list.
    List<String> strings = new ArrayList<String>();
    for (Path filePath : paths) {
      if (filePath.getName().startsWith("_") || filePath.getName().startsWith(".")) {
        continue;
      }

      // Need to use new configuration object so that it has the proper classloaders.
      SequenceFile.Reader reader = new SequenceFile.Reader(fs, filePath, new Configuration());
      WritableComparable key = (WritableComparable)
          reader.getKeyClass().newInstance();
      Writable value = (Writable) reader.getValueClass().newInstance();
      while (reader.next(key, value)) {
        strings.add(value.toString());
      }
    }

    ClassLoaderStack.setCurrentClassLoader(prevClassLoader);
    return strings.toArray(new String[0]);
  }

  public void testDeleteTargetDir() throws Exception {
    // Make sure that if a MapReduce job to do the import fails due
    // to an IOException, we tell the user about it.

    // Create a table to attempt to import.
    createTableForColType("VARCHAR(32)", "'meep'");

    Configuration conf = new Configuration();

    // Make the output dir does not exist
    Path outputPath = new Path(new Path(getWarehouseDir()), getTableName());
    FileSystem fs = FileSystem.getLocal(conf);
    fs.delete(outputPath, true);
    assertTrue(!fs.exists(outputPath));

    String[] argv = getArgv(true, new String[] { "DATA_COL0" }, conf);
    argv = Arrays.copyOf(argv, argv.length + 1);
    argv[argv.length - 1] = "--delete-target-dir";

    Sqoop importer = new Sqoop(new ImportTool());
    try {
      int ret = Sqoop.runSqoop(importer, argv);
      assertTrue("Expected job to go through if target directory"
        + " does not exist.", 0 == ret);
      assertTrue(fs.exists(outputPath));
      // expecting one _SUCCESS file and one file containing data
      assertTrue("Expecting two files in the directory.",
          fs.listStatus(outputPath).length == 2);
      String[] output = getContent(conf, outputPath);
      assertEquals("Expected output and actual output should be same.", "meep\n",
          output[0]);

      ret = Sqoop.runSqoop(importer, argv);
      assertTrue("Expected job to go through if target directory exists.",
        0 == ret);
      assertTrue(fs.exists(outputPath));
      // expecting one _SUCCESS file and one file containing data
      assertTrue("Expecting two files in the directory.",
          fs.listStatus(outputPath).length == 2);
      output = getContent(conf, outputPath);
      assertEquals("Expected output and actual output should be same.", "meep\n",
          output[0]);
    } catch (Exception e) {
      // In debug mode, ImportException is wrapped in RuntimeException.
      LOG.info("Got exceptional return (expected: ok). msg is: " + e);
    }
  }

  public void testManyColumns() throws Exception {
    int numberOfColumns = 7500;

    // Create a bunch of columns
    String[] colNames = new String[numberOfColumns];
    String[] colTypes = new String[numberOfColumns];
    String[] colVals = new String[numberOfColumns];
    List<String> testColVals = new ArrayList<String>(numberOfColumns);
    for (int i = 0; i < numberOfColumns; ++i) {
      colNames[i] = BASE_COL_NAME + Integer.toString(i);
      colTypes[i] = "VARCHAR(32)";
      colVals[i] = "'meep'";
      testColVals.add("meep");
    }
    createTableWithColTypesAndNames(colNames, colTypes, colVals);

    Configuration conf = new Configuration();

    // Make sure the output dir does not exist
    Path outputPath = new Path(new Path(getWarehouseDir()), getTableName());
    FileSystem fs = FileSystem.getLocal(conf);
    fs.delete(outputPath, true);
    assertTrue(!fs.exists(outputPath));

    String[] argv = getArgv(true, colNames, conf);

    Sqoop importer = new Sqoop(new ImportTool());
    try {
      int ret = Sqoop.runSqoop(importer, argv);
      assertTrue("Expected job to go through if target directory"
          + " does not exist.", 0 == ret);
      assertTrue(fs.exists(outputPath));
      // expecting one _SUCCESS file and one file containing data
      assertTrue("Expecting two files in the directory.",
          fs.listStatus(outputPath).length == 2);
      String[] output = getContent(conf, outputPath);
      assertEquals("Expected output and actual output should be same.",
          StringUtils.join(",", testColVals) + "\n",
          output[0]);
    } catch (Exception e) {
      // In debug mode, ImportException is wrapped in RuntimeException.
      LOG.info("Got exceptional return (expected: ok). msg is: " + e);
    }
  }
}
