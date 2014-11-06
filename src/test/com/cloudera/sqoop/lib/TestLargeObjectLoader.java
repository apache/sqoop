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

package com.cloudera.sqoop.lib;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.cloudera.sqoop.testutil.BaseSqoopTestCase;
import com.cloudera.sqoop.testutil.CommonArgs;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.sqoop.testutil.MockResultSet;

/**
 * Test deserialization of ClobRef and BlobRef fields.
 */
public class TestLargeObjectLoader extends TestCase {

  protected Configuration conf;
  protected LargeObjectLoader loader;
  protected Path outDir;

  public void setUp() throws IOException, InterruptedException {
    conf = new Configuration();
    if (!BaseSqoopTestCase.isOnPhysicalCluster()) {
      conf.set(CommonArgs.FS_DEFAULT_NAME, CommonArgs.LOCAL_FS);
    }
    String tmpDir = System.getProperty("test.build.data", "/tmp/");
    this.outDir = new Path(System.getProperty("java.io.tmpdir"));
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(outDir)) {
      fs.delete(outDir, true);
    }
    fs.mkdirs(outDir);

    loader = new LargeObjectLoader(conf, outDir);
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

