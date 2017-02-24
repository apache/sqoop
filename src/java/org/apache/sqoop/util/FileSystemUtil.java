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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public final class FileSystemUtil {
  private FileSystemUtil() {
  }


  /**
   * Creates a fully-qualified path object.
   * @param path the path to fully-qualify with its file system URI.
   * @param conf the current Hadoop configuration.
   * @return a new path representing the same location as the input path,
   * but with a fully-qualified URI. Returns {@code null} if provided path is {@code null};
   */
  public static Path makeQualified(Path path, Configuration conf)
      throws IOException {
    if (null == path) {
      return null;
    }

    return path.getFileSystem(conf).makeQualified(path);
  }
}
