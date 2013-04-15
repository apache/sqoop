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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.util.StringUtils;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;

/**
 * Test the named fifo utility.
 */
public class TestNamedFifo extends TestCase {

  public static final Log LOG = LogFactory.getLog(
        TestNamedFifo.class.getName());

  public static final Path TEMP_BASE_DIR;

  static {
    String tmpDir = System.getProperty("test.build.data", "/tmp/");
    if (!tmpDir.endsWith(File.separator)) {
      tmpDir = tmpDir + File.separator;
    }

    TEMP_BASE_DIR = new Path(new Path(tmpDir), "namedfifo");
  }

  private Configuration conf;
  private FileSystem fs;

  public void setUp() throws Exception {
    conf = new Configuration();
    conf.set("fs.default.name", "file:///");

    fs = FileSystem.getLocal(conf);
    fs.mkdirs(TEMP_BASE_DIR);
  }

  static final String MSG = "THIS IS THE MESSAGE\n";
  static final String MSG2 = "Here is a follow-up.\n";

  private static class ReaderThread extends Thread {
    private File file;
    private IOException exception;

    public ReaderThread(File f) {
      this.file = f;
    }

    /** return any exception during the run method. */
    public IOException getException() {
      return this.exception;
    }

    public void run() {
      BufferedReader r = null;
      try {
        r = new BufferedReader(new InputStreamReader(
            new FileInputStream(file)));

        // Assert that after a flush, we get back what we wrote.
        String line = r.readLine();
        if (!MSG.trim().equals(line)) {
          throw new IOException("Expected " + MSG.trim() + " but got "
              + line);
        }

        // Assert that after closing the writer, we get back what
        // we wrote again.
        line = r.readLine();
        if (null == line) {
          throw new IOException("line2 was null");
        } else if (!MSG2.trim().equals(line)) {
          throw new IOException("Expected " + MSG2.trim() + " but got "
              + line);
        }
      } catch (IOException ioe) {
        this.exception = ioe;
      } finally {
        if (null != r) {
          try {
            r.close();
          } catch (IOException ioe) {
            LOG.warn("Error closing reader: " + ioe);
          }
        }
      }
    }
  }

  private static class WriterThread extends Thread {
    private File file;
    private IOException exception;

    public WriterThread(File f) {
      this.file = f;
    }

    /** return any exception during the run method. */
    public IOException getException() {
      return this.exception;
    }

    public void run() {
      BufferedWriter w = null;
      try {
        w = new BufferedWriter(new OutputStreamWriter(
            new FileOutputStream(file)));

        w.write(MSG);
        w.flush();

        w.write(MSG2);
      } catch (IOException ioe) {
        this.exception = ioe;
      } finally {
        if (null != w) {
          try {
            w.close();
          } catch (IOException ioe) {
            LOG.warn("Error closing writer: " + ioe);
          }
        }
      }
    }
  }

  public void testNamedFifo() throws Exception {

    if (Shell.WINDOWS) {
      // NamedFifo uses Linux specific commands like mknod
      // and mkfifo, so skip the test on Windows OS
      LOG.warn("Named FIFO is not supported on Windows. Skipping test");
      return;
    }

    File root = new File(TEMP_BASE_DIR.toString());
    File fifo = new File(root, "foo-fifo");

    NamedFifo nf = new NamedFifo(fifo);
    nf.create();

    File returned = nf.getFile();

    // These should be the same object.
    assertEquals(fifo, returned);

    ReaderThread rt = new ReaderThread(returned);
    WriterThread wt = new WriterThread(returned);

    rt.start();
    wt.start();

    rt.join();
    wt.join();

    IOException rex = rt.getException();
    IOException wex = wt.getException();

    if (null != rex) {
      LOG.error("reader exception: " + StringUtils.stringifyException(rex));
    }

    if (null != wex) {
      LOG.error("writer exception: " + StringUtils.stringifyException(wex));
    }

    assertNull(rex);
    assertNull(wex);
  }
}

