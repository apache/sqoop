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

package com.cloudera.sqoop;

import com.cloudera.sqoop.orm.CompilationManager;
import com.cloudera.sqoop.testutil.BaseSqoopTestCase;
import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.HsqldbTestServer;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import com.cloudera.sqoop.testutil.SeqFileReader;
import com.cloudera.sqoop.tool.ImportTool;
import com.cloudera.sqoop.util.ClassLoaderStack;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Test that compression options (--compress, --compression-codec) work.
 */
public class TestCompression extends ImportJobTestCase {

  /**
   * Create the argv to pass to Sqoop.
   * @return the argv as an array of strings.
   */
  protected String [] getArgv(boolean includeHadoopFlags, String [] colNames,
      CompressionCodec codec, String fileFormat) {
    String columnsString = "";
    for (String col : colNames) {
      columnsString += col + ",";
    }

    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
    }

    args.add("--table");
    args.add(HsqldbTestServer.getTableName());
    args.add("--columns");
    args.add(columnsString);
    args.add("--compress");
    if (codec != null) {
      args.add("--compression-codec");
      args.add(codec.getClass().getName());
    }
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(HsqldbTestServer.getUrl());
    args.add(fileFormat);
    args.add("--num-mappers");
    args.add("1");

    return args.toArray(new String[0]);
  }

  // this test just uses the two int table.
  protected String getTableName() {
    return HsqldbTestServer.getTableName();
  }

  public void runSequenceFileCompressionTest(CompressionCodec codec,
      int expectedNum) throws Exception {

    String [] columns = HsqldbTestServer.getFieldNames();
    ClassLoader prevClassLoader = null;
    SequenceFile.Reader reader = null;

    String [] argv = getArgv(true, columns, codec, "--as-sequencefile");
    runImport(argv);
    try {
      SqoopOptions opts = new ImportTool().parseArguments(
          getArgv(false, columns, codec, "--as-sequencefile"),
          null, null, true);

      CompilationManager compileMgr = new CompilationManager(opts);
      String jarFileName = compileMgr.getJarFilename();
      LOG.debug("Got jar from import job: " + jarFileName);

      prevClassLoader = ClassLoaderStack.addJarFile(jarFileName,
          getTableName());

      reader = SeqFileReader.getSeqFileReader(getDataFilePath().toString());

      if (codec == null) {
        codec = new GzipCodec();
      }
      assertTrue("Block compressed", reader.isBlockCompressed());
      assertEquals(codec.getClass(), reader.getCompressionCodec().getClass());

      // here we can actually instantiate (k, v) pairs.
      Configuration conf = new Configuration();
      Object key = ReflectionUtils.newInstance(reader.getKeyClass(), conf);
      Object val = ReflectionUtils.newInstance(reader.getValueClass(), conf);

      // We know that these values are two ints separated by a ',' character.
      // Since this is all dynamic, though, we don't want to actually link
      // against the class and use its methods. So we just parse this back
      // into int fields manually.  Sum them up and ensure that we get the
      // expected total for the first column, to verify that we got all the
      // results from the db into the file.

      // Sum up everything in the file.
      int numLines = 0;
      while (reader.next(key) != null) {
        reader.getCurrentValue(val);
        numLines++;
      }

      assertEquals(expectedNum, numLines);
    } finally {
      IOUtils.closeStream(reader);

      if (null != prevClassLoader) {
        ClassLoaderStack.setCurrentClassLoader(prevClassLoader);
      }
    }
  }

  public void runTextCompressionTest(CompressionCodec codec, int expectedNum)
    throws IOException {

    String [] columns = HsqldbTestServer.getFieldNames();
    String [] argv = getArgv(true, columns, codec, "--as-textfile");
    runImport(argv);

    Configuration conf = new Configuration();
    if (!BaseSqoopTestCase.isOnPhysicalCluster()) {
      conf.set(CommonArgs.FS_DEFAULT_NAME, CommonArgs.LOCAL_FS);
    }
    FileSystem fs = FileSystem.get(conf);

    if (codec == null) {
      codec = new GzipCodec();
    }
    ReflectionUtils.setConf(codec, getConf());
    Path p = new Path(getDataFilePath().toString()
        + codec.getDefaultExtension());
    InputStream is = codec.createInputStream(fs.open(p));
    BufferedReader r = new BufferedReader(new InputStreamReader(is));
    int numLines = 0;
    while (true) {
      String ln = r.readLine();
      if (ln == null) {
        break;
      }
      numLines++;
    }
    r.close();
    assertEquals(expectedNum, numLines);
  }

  public void testDefaultTextCompression() throws IOException {
    runTextCompressionTest(null, 4);
  }

  public void testBzip2TextCompression() throws IOException {
    runTextCompressionTest(new BZip2Codec(), 4);
  }

  public void testBzip2SequenceFileCompression() throws Exception {
    runSequenceFileCompressionTest(new BZip2Codec(), 4);
  }
}
