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

import java.io.*;

import com.cloudera.sqoop.testutil.BaseSqoopTestCase;
import com.cloudera.sqoop.testutil.CommonArgs;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.cloudera.sqoop.io.LobFile;

/**
 * Test that the BlobRef.parse() method does the right thing.
 * Note that we don't support inline parsing here; we only expect this to
 * really work for external BLOBs.
 */
public class TestBlobRef extends TestCase {

  public void testEmptyStr() {
    BlobRef r = BlobRef.parse("");
    assertFalse(r.isExternal());
  }

  public void testInline() throws IOException {
    BlobRef r = BlobRef.parse("foo");
    assertFalse(r.isExternal());
  }

  public void testEmptyFile() {
    BlobRef r = BlobRef.parse("externalLob()");
    assertFalse(r.isExternal());

    r = BlobRef.parse("externalLob(lf,,0,0)");
    assertTrue(r.isExternal());
    assertEquals("externalLob(lf,,0,0)", r.toString());
  }

  public void testInlineNearMatch() {
    BlobRef r = BlobRef.parse("externalLob(foo)bar");
    assertFalse(r.isExternal());

    r = BlobRef.parse("externalLob(foo)");
    assertFalse(r.isExternal());

    r = BlobRef.parse("externalLob(lf,foo)");
    assertFalse(r.isExternal());

    r = BlobRef.parse("externalLob(lf,foo,1,2)x");
    assertFalse(r.isExternal());
  }

  public void testExternal() throws IOException {
    final byte [] DATA = { 1, 2, 3, 4, 5 };
    final String FILENAME = "blobdata";

    doExternalTest(DATA, FILENAME);
  }

  public void testExternalSubdir() throws IOException {
    final byte [] DATA = { 1, 2, 3, 4, 5 };
    final String FILENAME = "_lob/blobdata";

    try {
      doExternalTest(DATA, FILENAME);
    } finally {
      // remove dir we made.
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.getLocal(conf);
      String tmpDir = System.getProperty("test.build.data", "/tmp/");
      Path lobDir = new Path(new Path(tmpDir), "_lob");
      fs.delete(lobDir, true);
    }
  }

  private void doExternalTest(final byte [] data, final String filename)
      throws IOException {

    Configuration conf = new Configuration();
    if (!BaseSqoopTestCase.isOnPhysicalCluster()) {
      conf.set(CommonArgs.FS_DEFAULT_NAME, CommonArgs.LOCAL_FS);
    }
    FileSystem fs = FileSystem.get(conf);
    String tmpDir = System.getProperty("test.build.data", "/tmp/");

    Path tmpPath = new Path(tmpDir);
    Path blobFile = new Path(tmpPath, filename);

    // make any necessary parent dirs.
    Path blobParent = blobFile.getParent();
    if (!fs.exists(blobParent)) {
      fs.mkdirs(blobParent);
    }

    LobFile.Writer lw = LobFile.create(blobFile, conf, false);
    try {
      long off = lw.tell();
      long len = data.length;
      OutputStream os = lw.writeBlobRecord(len);
      os.write(data, 0, data.length);
      os.close();
      lw.close();

      String refString = "externalLob(lf," + filename
          + "," + off + "," + len + ")";
      BlobRef blob = BlobRef.parse(refString);
      assertTrue(blob.isExternal());
      assertEquals(refString, blob.toString());
      InputStream is = blob.getDataStream(conf, tmpPath);
      assertNotNull(is);

      byte [] buf = new byte[4096];
      int bytes = is.read(buf, 0, 4096);
      is.close();

      assertEquals(data.length, bytes);
      for (int i = 0; i < bytes; i++) {
        assertEquals(data[i], buf[i]);
      }
    } finally {
      fs.delete(blobFile, false);
    }
  }
}

