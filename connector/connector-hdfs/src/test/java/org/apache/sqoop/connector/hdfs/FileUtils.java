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
package org.apache.sqoop.connector.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

public class FileUtils {

  public static boolean exists(String file) throws IOException {
    Path path = new Path(file);
    FileSystem fs = path.getFileSystem(new Configuration());
    return fs.exists(path);
  }

  public static void delete(String file) throws IOException {
    Path path = new Path(file);
    FileSystem fs = path.getFileSystem(new Configuration());
    if (fs.exists(path)) {
      fs.delete(path, true);
    }
  }

  public static void mkdirs(String directory) throws IOException {
    Path path = new Path(directory);
    FileSystem fs = path.getFileSystem(new Configuration());
    if (!fs.exists(path)) {
      fs.mkdirs(path);
    }
  }

  public static InputStream open(String fileName)
    throws IOException, ClassNotFoundException {
    Path filepath = new Path(fileName);
    FileSystem fs = filepath.getFileSystem(new Configuration());
    return fs.open(filepath);
  }

  public static OutputStream create(String fileName) throws IOException {
    Path filepath = new Path(fileName);
    FileSystem fs = filepath.getFileSystem(new Configuration());
    return fs.create(filepath, false);
  }

  public static Path[] listDir(String directory) throws IOException {
    Path dirpath = new Path(directory);
    FileSystem fs = dirpath.getFileSystem(new Configuration());
    List<Path> paths = new LinkedList<Path>();
    for (FileStatus fileStatus : fs.listStatus(dirpath)) {
      paths.add(fileStatus.getPath());
    }
    return paths.toArray(new Path[paths.size()]);
  }

  private FileUtils() {
    // Disable explicit object creation
  }

}
