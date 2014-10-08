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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * A password loader that loads an credential provider alias.
 * The alias is  resolved using the Hadoop credential provider facilitity
 * if available.
 */
public class CredentialProviderPasswordLoader extends FilePasswordLoader {
  public static final Log LOG =
    LogFactory.getLog(CredentialProviderPasswordLoader.class.getName());

  /**
   * If credential provider is available (made available as part of 2.6.0 and
   * 3.0, then use the credential provider to get the password. Else throw an
   * exception saying this provider is not available.
   */
  @Override
  public String loadPassword(String p, Configuration configuration)
    throws IOException {
    if (!CredentialProviderHelper.isProviderAvailable()) {
      throw new IOException("CredentialProvider facility not available "
        + "in the hadoop environment used");
    }
    LOG.debug("Fetching alias from the specified path: " + p);
    Path path = new Path(p);
    FileSystem fs = path.getFileSystem(configuration);

    // Not closing FileSystem object because of SQOOP-1226
    verifyPath(fs, path);
    String alias = new String(readBytes(fs, path));
    String pass = CredentialProviderHelper.resolveAlias(configuration, alias);
    return pass;
  }
}
