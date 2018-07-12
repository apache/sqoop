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

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;

/**
 * This Hook is currently used to clean up the temp directory that is used
 * to hold the generated JAR files for each table class under /tmp/sqoop-<username>/compile.
 *
 * But it is generic so it can also be used to clean up other directories if needed
 */
public class DirCleanupHook extends Thread {

  private File dir;

  public static final Log LOG = LogFactory.getLog(DirCleanupHook.class.getName());

  public DirCleanupHook(String dirPath) {
    dir = new File(dirPath);
  }

  public void run() {
    try {
      LOG.debug("Removing directory: " + dir + " in the clean up hook.");
      FileUtils.deleteDirectory(dir);
    } catch (IOException e) {
      LOG.error("Unable to remove directory: " + dir + ". Error was: " + e.getMessage());
    }
  }
}
