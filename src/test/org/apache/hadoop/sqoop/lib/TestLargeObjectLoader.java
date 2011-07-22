/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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

package org.apache.hadoop.sqoop.lib;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.sql.ResultSet;
import java.sql.SQLException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mrunit.mapreduce.mock.MockMapContext;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.hadoop.sqoop.testutil.MockResultSet;

/**
 * Test deserialization of ClobRef and BlobRef fields.
 */
public class TestLargeObjectLoader extends TestCase {

  /**
   * A mock MapContext that uses FileOutputCommitter.
   * This MapContext is actually serving two roles here; when writing the
   * CLOB files, its OutputCommitter is used to determine where to write
   * the CLOB data, as these are placed in the task output work directory.
   * When reading the CLOB data back for verification, we use the
   * getInputSplit() to determine where to read our source data from--the same
   * directory. We are repurposing the same context for both output and input.
   */
  private static class MockMapContextWithCommitter<K1, V1, K2, V2>
      extends MockMapContext<K1, V1, K2, V2> {
    private Path outputDir;
    private Configuration conf;

    public MockMapContextWithCommitter(Configuration conf, Path outDir) {
      super(new ArrayList<Pair<K1, V1>>(), new Counters());

      this.outputDir = outDir;
      this.conf = conf;
    }

    @Override
    public OutputCommitter getOutputCommitter() {
      try {
        return new FileOutputCommitter(outputDir, this);
      } catch (IOException ioe) {
        return null;
      }
    }

    @Override
    public InputSplit getInputSplit() {
      return new FileSplit(new Path(outputDir, "inputFile"), 0, 0, new String[0]);
    }

    @Override
    public Configuration getConfiguration() {
      return conf;
    }
  }

  protected Configuration conf;
  protected MapContext mapContext;
  protected LargeObjectLoader loader;

  public void setUp() throws IOException {
    conf = new Configuration();
    conf.set("fs.defaultFS", "file:///");
    String tmpDir = System.getProperty("test.build.data", "/tmp/");
    Path outDir = new Path(new Path(tmpDir), "testLobLoader");
    FileSystem fs = FileSystem.getLocal(conf);
    if (fs.exists(outDir)) {
      fs.delete(outDir, true);
    }
    fs.mkdirs(outDir);

    mapContext = new MockMapContextWithCommitter(conf, outDir);
    loader = new LargeObjectLoader(mapContext);
  }

  public void testReadClobRef()
      throws IOException, InterruptedException, SQLException {
    // This should give us an inline CLOB.
    ResultSet resultSet = new MockResultSet();
    ClobRef clob = loader.readClobRef(0, resultSet);
    assertNotNull(clob);
    assertFalse(clob.isExternal());
    assertEquals(MockResultSet.CLOB_DATA, clob.toString());

    // LOBs bigger than 4 bytes are now external.
    conf.setLong(LargeObjectLoader.MAX_INLINE_LOB_LEN_KEY, 4);
    clob = loader.readClobRef(0, resultSet);
    assertNotNull(clob);
    assertTrue(clob.isExternal());
    mapContext.getOutputCommitter().commitTask(mapContext);
    Reader r = clob.getDataReader(mapContext);
    char [] buf = new char[4096];
    int chars = r.read(buf, 0, 4096);
    r.close();
    String str = new String(buf, 0, chars);
    assertEquals(MockResultSet.CLOB_DATA, str);
  }

  public void testReadBlobRef()
      throws IOException, InterruptedException, SQLException {
    // This should give us an inline BLOB.
    ResultSet resultSet = new MockResultSet();
    BlobRef blob = loader.readBlobRef(0, resultSet);
    assertNotNull(blob);
    assertFalse(blob.isExternal());
    byte [] data = blob.getData();
    assertEquals(MockResultSet.BLOB_DATA.length, data.length);
    for (int i = 0; i < data.length; i++) {
      assertEquals(MockResultSet.BLOB_DATA[i], data[i]);
    }

    // LOBs bigger than 4 bytes are now external.
    conf.setLong(LargeObjectLoader.MAX_INLINE_LOB_LEN_KEY, 4);
    blob = loader.readBlobRef(0, resultSet);
    assertNotNull(blob);
    assertTrue(blob.isExternal());
    mapContext.getOutputCommitter().commitTask(mapContext);
    InputStream is = blob.getDataStream(mapContext);
    byte [] buf = new byte[4096];
    int bytes = is.read(buf, 0, 4096);
    is.close();

    assertEquals(MockResultSet.BLOB_DATA.length, bytes);
    for (int i = 0; i < bytes; i++) {
      assertEquals(MockResultSet.BLOB_DATA[i], buf[i]);
    }
  }
}

