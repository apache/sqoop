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

package org.apache.sqoop.util;

import java.io.IOException;
import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.conf.Configuration;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.io.CodecMap;
import com.cloudera.sqoop.io.SplittingOutputStream;
import com.cloudera.sqoop.io.SplittableBufferedWriter;

import org.apache.hadoop.util.Shell;
import com.cloudera.sqoop.manager.ImportJobContext;

/**
 * Utility methods that are common to various the direct import managers.
 */
public final class DirectImportUtils {

  public static final Log LOG = LogFactory.getLog(
      DirectImportUtils.class.getName());

  private DirectImportUtils() {
  }

  /**
   * Executes chmod on the specified file, passing in the mode string 'modstr'
   * which may be e.g. "a+x" or "0600", etc.
   * @throws IOException if chmod failed.
   */
  public static void setFilePermissions(File file, String modstr)
      throws IOException {
    // Set this file to be 0600. Java doesn't have a built-in mechanism for this
    // so we need to go out to the shell to execute chmod.
    try {
      Shell.execCommand("chmod", modstr, file.toString());
    } catch (IOException ioe) {
      // Shell.execCommand will throw IOException on exit code != 0.
      LOG.error("Could not chmod " + modstr + " " + file.toString());
      throw new IOException("Could not ensure password file security.", ioe);
    }
  }

  /**
   * Open a file in HDFS for write to hold the data associated with a table.
   * Creates any necessary directories, and returns the OutputStream to write
   * to. The caller is responsible for calling the close() method on the
   * returned stream.
   */
  public static SplittableBufferedWriter createHdfsSink(Configuration conf,
      SqoopOptions options, ImportJobContext context) throws IOException {

    Path destDir = context.getDestination();
    FileSystem fs = destDir.getFileSystem(conf);

    LOG.debug("Writing to filesystem: " + fs.getUri());
    LOG.debug("Creating destination directory " + destDir);
    fs.mkdirs(destDir);

    // This Writer will be closed by the caller.
    return new SplittableBufferedWriter(
        new SplittingOutputStream(conf, destDir, "part-m-",
        options.getDirectSplitSize(), getCodec(conf, options)));
  }

  private static CompressionCodec getCodec(Configuration conf,
      SqoopOptions options) throws IOException {
    if (options.shouldUseCompression()) {
      if (options.getCompressionCodec() == null) {
        return new GzipCodec();
      } else {
        return CodecMap.getCodec(options.getCompressionCodec(), conf);
      }
    }
    return null;
  }

  /** @return true if someHost refers to localhost.
   */
  public static boolean isLocalhost(String someHost) {
    if (null == someHost) {
      return false;
    }

    try {
      InetAddress localHostAddr = InetAddress.getLocalHost();
      InetAddress someAddr = InetAddress.getByName(someHost);

      return localHostAddr.equals(someAddr);
    } catch (UnknownHostException uhe) {
      return false;
    }
  }

}

