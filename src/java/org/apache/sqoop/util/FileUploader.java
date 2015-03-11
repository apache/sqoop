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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileUploader {
  public static final Log LOG =
        LogFactory.getLog(FileUploader.class.getName());

  private FileUploader() { }

  public static void uploadFilesToDFS(String srcBasePath, String src,
    String destBasePath, String dest, Configuration conf) throws IOException {

    FileSystem fs = FileSystem.get(conf);
    Path targetPath = null;
    Path srcPath = new Path(srcBasePath, src);

    if (destBasePath == null || destBasePath.length() == 0) {
      destBasePath = ".";
    }

    targetPath = new Path(destBasePath, dest);

    if (!fs.exists(targetPath)) {
      fs.mkdirs(targetPath);
    }

    Path targetPath2 = new Path(targetPath, src);
    fs.delete(targetPath2, true);

    try {
      LOG.info("Copying " + srcPath + " to " + targetPath);
      // Copy srcPath (on local FS) to targetPath on DFS.
      // The first boolean arg instructs not to delete source and the second
      // boolean arg instructs to overwrite dest if exists.
      fs.copyFromLocalFile(false, true, srcPath, targetPath);
    } catch (IOException ioe) {
      LOG.warn("Unable to copy " + srcPath + " to " + targetPath);
    }
  }

}
