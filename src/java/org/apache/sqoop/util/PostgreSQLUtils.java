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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Utility methods for PostgreSQL import/export.
 */
public final class PostgreSQLUtils {

  public static final Log LOG =
    LogFactory.getLog(PostgreSQLUtils.class.getName());

  private PostgreSQLUtils() {
  }

  /** Write the user's password to a file that is chmod 0600.
      @return the filename.
  */
  public static String writePasswordFile(String tmpDir, String password)
    throws IOException {
    File tempFile = File.createTempFile("pgpass", ".pgpass", new File(tmpDir));
    LOG.debug("Writing password to tempfile: " + tempFile);

    // Make sure it's only readable by the current user.
    DirectImportUtils.setFilePermissions(tempFile, "0600");

    // Actually write the password data into the file.
    BufferedWriter w = new BufferedWriter(
        new OutputStreamWriter(new FileOutputStream(tempFile)));
    w.write("*:*:*:*:" + password);
    w.close();
    return tempFile.toString();
  }
}
