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

package com.cloudera.sqoop.lib;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.cloudera.sqoop.shims.HadoopShim;
import com.cloudera.sqoop.testutil.MockResultSet;

/**
 * Test deserialization of ClobRef and BlobRef fields.
 */
public class TestLargeObjectLoader extends TestCase {

  protected Configuration conf;
  protected LargeObjectLoader loader;
  protected Path outDir;
  protected MapContext mapContext;

  public void setUp() throws IOException, InterruptedException {
    conf = new Configuration();
    conf.set("fs.defaultFS", "file:///");
    String tmpDir = System.getProperty("test.build.data", "/tmp/");
    this.outDir = new Path(new Path(tmpDir), "testLobLoader");
    FileSystem fs = FileSystem.getLocal(conf);
    if (fs.exists(outDir)) {
      fs.delete(outDir, true);
    }
    fs.mkdirs(outDir);

    /* A mock MapContext that uses FileOutputCommitter.
     * This MapContext is actually serving two roles here; when writing the
     * CLOB files, its OutputCommitter is used to determine where to write
     * the CLOB data, as these are placed in the task output work directory.
     * When reading the CLOB data back for verification, we use the
     * getInputSplit() to determine where to read our source data from--the same
     * directory. We are repurposing the same context for both output and input.
     */
    mapContext = HadoopShim.get().getMapContextForIOPath(conf, outDir);
    loader = new LargeObjectLoader(mapContext.getConfiguration(),
        FileOutputFormat.getWorkOutputPath(mapContext));
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
    loader.close();
    mapContext.getOutputCommitter().commitTask(mapContext);
    Reader r = clob.getDataStream(conf, outDir);
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
    byte [] blobData = MockResultSet.blobData();
    assertEquals(blobData.length, data.length);
    for (int i = 0; i < data.length; i++) {
      assertEquals(blobData[i], data[i]);
    }

    // LOBs bigger than 4 bytes are now external.
    conf.setLong(LargeObjectLoader.MAX_INLINE_LOB_LEN_KEY, 4);
    blob = loader.readBlobRef(0, resultSet);
    assertNotNull(blob);
    assertTrue(blob.isExternal());
    loader.close();
    mapContext.getOutputCommitter().commitTask(mapContext);
    InputStream is = blob.getDataStream(conf, outDir);
    byte [] buf = new byte[4096];
    int bytes = is.read(buf, 0, 4096);
    is.close();

    assertEquals(blobData.length, bytes);
    for (int i = 0; i < bytes; i++) {
      assertEquals(blobData[i], buf[i]);
    }
  }
}

