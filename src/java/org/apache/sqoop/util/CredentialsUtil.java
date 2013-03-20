/**
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.util;

import com.cloudera.sqoop.SqoopOptions;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

/**
 * A utility class for fetching passwords from a file.
 */
public final class CredentialsUtil {

  public static final Log LOG = LogFactory.getLog(
    CredentialsUtil.class.getName());

  private CredentialsUtil() {
  }

  public static String fetchPasswordFromFile(SqoopOptions options)
    throws IOException {
    String passwordFilePath = options.getPasswordFilePath();
    if (passwordFilePath == null) {
      return options.getPassword();
    }

    return fetchPasswordFromFile(options.getConf(), passwordFilePath);
  }

  public static String fetchPasswordFromFile(Configuration conf,
                                             String passwordFilePath)
    throws IOException {
    LOG.debug("Fetching password from specified path: " + passwordFilePath);
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(passwordFilePath);

    if (!fs.exists(path)) {
      throw new IOException("The password file does not exist! "
        + passwordFilePath);
    }

    if (!fs.isFile(path)) {
      throw new IOException("The password file cannot be a directory! "
        + passwordFilePath);
    }

    InputStream is = fs.open(path);
    StringWriter writer = new StringWriter();
    try {
      IOUtils.copy(is, writer);
      return writer.toString();
    } finally {
      IOUtils.closeQuietly(is);
      IOUtils.closeQuietly(writer);
      fs.close();
    }
  }
}
