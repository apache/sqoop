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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.util.password.FilePasswordLoader;
import org.apache.sqoop.util.password.PasswordLoader;

import java.io.IOException;

/**
 * A utility class for fetching passwords from a file.
 */
public final class CredentialsUtil {

  /**
   * Property for specifying which loader should be used to fetch the password.
   */
  private static String PROPERTY_LOADER_CLASSS = "org.apache.sqoop.credentials.loader.class";

  /**
   * The default loader is a FilePasswordLoader that will fetch the password from a file.
   */
  private static String DEFAULT_PASSWORD_LOADER = FilePasswordLoader.class.getCanonicalName();

  public static final Log LOG = LogFactory.getLog(CredentialsUtil.class.getName());

  private CredentialsUtil() {
  }

  /**
   * Return password that was specified (either on command line or via other facilities).
   *
   * @param options Sqoop Options
   * @return Password
   * @throws IOException
   */
  public static String fetchPassword(SqoopOptions options)
    throws IOException {
    String passwordFilePath = options.getPasswordFilePath();
    if (passwordFilePath == null) {
      return options.getPassword();
    }

    return fetchPasswordFromLoader(options.getPasswordFilePath(), options.getConf());
  }

  /**
   * Return password via --password-file argument.
   *
   * Given loader can be overridden using PROPERTY_LOADER_CLASSS.
   *
   * @param path Path with the password file.
   * @param conf Configuration
   * @return Password
   * @throws IOException
   */
  public static String fetchPasswordFromLoader(String path, Configuration conf) throws IOException {
    PasswordLoader loader = getLoader(conf);
    return loader.loadPassword(path, conf);
  }

  /**
   * Remove any potentially sensitive information from the configuration object.
   *
   * @param configuration Associated configuration object.
   * @throws IOException
   */
  public static void cleanUpSensitiveProperties(Configuration configuration) throws IOException {
    PasswordLoader loader = getLoader(configuration);
    loader.cleanUpConfiguration(configuration);
  }

  /**
   * Instantiate configured PasswordLoader class.
   *
   * @param configuration Associated configuration object.
   * @return
   * @throws IOException
   */
  public static PasswordLoader getLoader(Configuration configuration) throws IOException {
    String loaderClass = configuration.get(PROPERTY_LOADER_CLASSS, DEFAULT_PASSWORD_LOADER);

    try {
      return (PasswordLoader) Class.forName(loaderClass).newInstance();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
