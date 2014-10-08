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
package org.apache.sqoop.util.password;

import org.apache.hadoop.conf.Configuration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.IOException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.InputStream;

/**
 * Simple password loader that will load file from file system.
 *
 * The file system can be either local or any remote implementation supported
 * by your Hadoop installation. As the password needs to be stored in the file
 * in a clear text, the password file should have very restrictive permissions
 * (400).
 */
public class FilePasswordLoader extends PasswordLoader {

  public static final Log LOG = LogFactory.getLog(FilePasswordLoader.class.getName());

  /**
   * Verify that given path leads to a file that we can read.
   *
   * @param fs Associated FileSystem
   * @param path Path
   * @throws IOException
   */
  protected void verifyPath(FileSystem fs, Path path) throws IOException {
    if (!fs.exists(path)) {
      throw new IOException("The provided password file " + path
        + " does not exist!");
    }

    if (!fs.isFile(path)) {
      throw new IOException("The provided password file " + path
        + " is a directory!");
    }
  }

  /**
   * Read bytes from given file.
   *
   * @param fs Associated FileSystem
   * @param path Path
   * @return
   * @throws IOException
   */
  protected byte [] readBytes(FileSystem fs, Path path) throws IOException {
    InputStream is = fs.open(path);
    try {
      return IOUtils.toByteArray(is);
    } finally {
      IOUtils.closeQuietly(is);
    }
  }

  @Override
  public String loadPassword(String p, Configuration configuration) throws IOException {
    LOG.debug("Fetching password from specified path: " + p);
    Path path = new Path(p);
    FileSystem fs = path.getFileSystem(configuration);

    // Not closing FileSystem object because of SQOOP-1226
    verifyPath(fs, path);
    return new String(readBytes(fs, path));
  }
}
