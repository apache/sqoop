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

package com.cloudera.sqoop.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.File;
import java.io.FileInputStream;
import java.util.zip.GZIPInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.GzipCodec;

import com.cloudera.sqoop.testutil.ImportJobTestCase;

import junit.framework.TestCase;

/**
 * Test that the splittable buffered writer system works.
 */
public class TestSplittableBufferedWriter extends TestCase {

  public static final Log LOG = LogFactory.getLog(
      TestSplittableBufferedWriter.class.getName());

  private String getWriteDir() {
    return new File(ImportJobTestCase.TEMP_BASE_DIR,
        "bufferedWriterTest").toString();
  }

  private Path getWritePath() {
    return new Path(ImportJobTestCase.TEMP_BASE_DIR, "bufferedWriterTest");
  }

  /** Create the directory where we'll write our test files to; and
   * make sure it has no files in it.
   */
  private void ensureEmptyWriteDir() throws IOException {
    FileSystem fs = FileSystem.getLocal(getConf());
    Path writeDir = getWritePath();

    fs.mkdirs(writeDir);

    FileStatus [] stats = fs.listStatus(writeDir);

    for (FileStatus stat : stats) {
      if (stat.isDir()) {
        fail("setUp(): Write directory " + writeDir
            + " contains subdirectories");
      }

      LOG.debug("setUp(): Removing " + stat.getPath());
      if (!fs.delete(stat.getPath(), false)) {
        fail("setUp(): Could not delete residual file " + stat.getPath());
      }
    }

    if (!fs.exists(writeDir)) {
      fail("setUp: Could not create " + writeDir);
    }
  }

  public void setUp() throws IOException {
    ensureEmptyWriteDir();
  }

  private Configuration getConf() {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    return conf;
  }

  /** Verifies contents of an InputStream. Closes the InputStream on
    * its way out. Fails the test if the file doesn't match the expected set
    * of lines.
    */
  private void verifyFileContents(InputStream is, String [] lines)
      throws IOException {
    BufferedReader r = new BufferedReader(new InputStreamReader(is));
    try {
      for (String expectedLine : lines) {
        String actualLine = r.readLine();
        assertNotNull(actualLine);
        assertEquals("Input line mismatch", expectedLine, actualLine);
      }

      assertNull("Stream had additional contents after expected line",
          r.readLine());
    } finally {
      r.close();
      try {
        is.close();
      } catch (IOException ioe) {
        // ignore IOE; may be closed by reader.
      }
    }
  }

  private void verifyFileExists(Path p) throws IOException {
    FileSystem fs = FileSystem.getLocal(getConf());
    assertTrue("File not found: " + p, fs.exists(p));
  }

  private void verifyFileDoesNotExist(Path p) throws IOException {
    FileSystem fs = FileSystem.getLocal(getConf());
    assertFalse("File found: " + p + " and we did not expect it", fs.exists(p));
  }

  public void testNonSplittingTextFile() throws IOException {
    SplittingOutputStream os  = new SplittingOutputStream(getConf(),
        getWritePath(), "nonsplit-", 0, null);
    try {
      SplittableBufferedWriter w = new SplittableBufferedWriter(os, true);
      try {
        w.allowSplit();
        w.write("This is a string!");
        w.newLine();
        w.write("This is another string!");
        w.allowSplit();
      } finally {
        w.close();
      }
    } finally {
      try {
        os.close();
      } catch (IOException ioe) {
        // Ignored; may be thrown because w is already closed.
      }
    }

    // Ensure we made exactly one file.
    Path writePath = new Path(getWritePath(), "nonsplit-00000");
    Path badPath = new Path(getWritePath(), "nonsplit-00001");
    verifyFileExists(writePath);
    verifyFileDoesNotExist(badPath); // Ensure we didn't make a second file.

    // Now ensure all the data got there.
    String [] expectedLines = {
      "This is a string!",
      "This is another string!",
    };

    InputStream fis = new FileInputStream(new File(getWriteDir(),
          "nonsplit-00000"));
    try {
      verifyFileContents(fis, expectedLines);
    } finally {
      try {
        fis.close();
      } catch (IOException ioe) {
        // Ignored; may be closed by verifyFileContents().
      }
    }
  }

  public void testNonSplittingGzipFile() throws IOException {
    SplittingOutputStream os  = new SplittingOutputStream(getConf(),
        getWritePath(), "nonsplit-", 0, new GzipCodec());
    SplittableBufferedWriter w = new SplittableBufferedWriter(os, true);
    try {
      w.allowSplit();
      w.write("This is a string!");
      w.newLine();
      w.write("This is another string!");
      w.allowSplit();
    } finally {
      w.close();
    }

    // Ensure we made exactly one file.
    Path writePath = new Path(getWritePath(), "nonsplit-00000.gz");
    Path badPath = new Path(getWritePath(), "nonsplit-00001.gz");
    verifyFileExists(writePath);
    verifyFileDoesNotExist(badPath); // Ensure we didn't make a second file.

    // Now ensure all the data got there.
    String [] expectedLines = {
      "This is a string!",
      "This is another string!",
    };
    verifyFileContents(
        new GZIPInputStream(new FileInputStream(new File(getWriteDir(),
        "nonsplit-00000.gz"))), expectedLines);
  }

  public void testSplittingTextFile() throws IOException {
    SplittingOutputStream os  = new SplittingOutputStream(getConf(),
        getWritePath(), "split-", 10, null);
    try {
      SplittableBufferedWriter w = new SplittableBufferedWriter(os, true);
      try {
        w.allowSplit();
        w.write("This is a string!");
        w.newLine();
        w.write("This is another string!");
      } finally {
        w.close();
      }
    } finally {
      try {
        os.close();
      } catch (IOException ioe) {
        // Ignored; may be thrown because w is already closed.
      }
    }

    // Ensure we made exactly two files.
    Path writePath = new Path(getWritePath(), "split-00000");
    Path writePath2 = new Path(getWritePath(), "split-00001");
    Path badPath = new Path(getWritePath(), "split-00002");
    verifyFileExists(writePath);
    verifyFileExists(writePath2);
    verifyFileDoesNotExist(badPath); // Ensure we didn't make three files.

    // Now ensure all the data got there.
    String [] expectedLines0 = {
      "This is a string!",
    };
    InputStream fis = new FileInputStream(new File(getWriteDir(),
        "split-00000"));
    try {
      verifyFileContents(fis, expectedLines0);
    } finally {
      try {
        fis.close();
      } catch (IOException ioe) {
        // ignored; may be generated because fis closed in verifyFileContents.
      }
    }

    String [] expectedLines1 = {
      "This is another string!",
    };
    fis = new FileInputStream(new File(getWriteDir(), "split-00001"));
    try {
      verifyFileContents(fis, expectedLines1);
    } finally {
      try {
        fis.close();
      } catch (IOException ioe) {
        // Ignored; may be thrown because it's closed in verifyFileContents.
      }
    }
  }

  public void testSplittingGzipFile() throws IOException {
    SplittingOutputStream os = new SplittingOutputStream(getConf(),
        getWritePath(), "splitz-", 3, new GzipCodec());
    SplittableBufferedWriter w = new SplittableBufferedWriter(os, true);
    try {
      w.write("This is a string!");
      w.newLine();
      w.write("This is another string!");
    } finally {
      w.close();
    }

    // Ensure we made exactly two files.
    Path writePath = new Path(getWritePath(), "splitz-00000.gz");
    Path writePath2 = new Path(getWritePath(), "splitz-00001.gz");
    Path badPath = new Path(getWritePath(), "splitz-00002.gz");
    verifyFileExists(writePath);
    verifyFileExists(writePath2);
    verifyFileDoesNotExist(badPath); // Ensure we didn't make three files.

    // Now ensure all the data got there.
    String [] expectedLines0 = {
      "This is a string!",
    };
    verifyFileContents(
        new GZIPInputStream(new FileInputStream(new File(getWriteDir(),
        "splitz-00000.gz"))), expectedLines0);

    String [] expectedLines1 = {
      "This is another string!",
    };
    verifyFileContents(
        new GZIPInputStream(new FileInputStream(new File(getWriteDir(),
        "splitz-00001.gz"))), expectedLines1);
  }
}
