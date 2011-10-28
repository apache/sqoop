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
package org.apache.sqoop.io;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.sqoop.io.LobFile;

/**
 * A cache of open LobFile.Reader objects.
 * This maps from filenames to the open Reader, if any.  This uses the
 * Singleton pattern. While nothing prevents multiple LobReaderCache
 * instances, it is most useful to have a single global cache. This cache is
 * internally synchronized; only one thread can insert or retrieve a reader
 * from the cache at a time.
 */
public class LobReaderCache {

  public static final Log LOG =
      LogFactory.getLog(LobReaderCache.class.getName());

  private Map<Path, LobFile.Reader> readerMap;

  /**
   * Open a LobFile for read access, returning a cached reader if one is
   * available, or a new reader otherwise.
   * @param path the path to the LobFile to open
   * @param conf the configuration to use to access the FS.
   * @throws IOException if there's an error opening the file.
   */
  public LobFile.Reader get(Path path, Configuration conf)
      throws IOException {

    LobFile.Reader reader = null;
    Path canonicalPath = qualify(path, conf);
    // Look up an entry in the cache.
    synchronized(this) {
      reader = readerMap.remove(canonicalPath);
    }

    if (null != reader && !reader.isClosed()) {
      // Cache hit. return it.
      LOG.debug("Using cached reader for " + canonicalPath);
      return reader;
    }

    // Cache miss; open the file.
    LOG.debug("No cached reader available for " + canonicalPath);
    return LobFile.open(path, conf);
  }

  /**
   * Return a reader back to the cache. If there's already a reader for
   * this path, then the current reader is closed.
   * @param reader the opened reader. Any record-specific subreaders should be
   * closed.
   * @throws IOException if there's an error accessing the path's filesystem.
   */
  public void recycle(LobFile.Reader reader) throws IOException {
    Path canonicalPath = reader.getPath();

    // Check if the cache has a reader for this path already. If not, add this.
    boolean cached = false;
    synchronized(this) {
      if (readerMap.get(canonicalPath) == null) {
        LOG.debug("Caching reader for path: " + canonicalPath);
        readerMap.put(canonicalPath, reader);
        cached = true;
      }
    }

    if (!cached) {
      LOG.debug("Reader already present for path: " + canonicalPath
          + "; closing.");
      reader.close();
    }
  }

  @Override
  protected synchronized void finalize() throws Throwable {
    for (LobFile.Reader r : readerMap.values()) {
      r.close();
    }

    super.finalize();
  }

  protected LobReaderCache() {
    this.readerMap = new TreeMap<Path, LobFile.Reader>();
  }

  /**
   * Created a fully-qualified path object.
   * @param path the path to fully-qualify with its fs URI.
   * @param conf the current Hadoop FS configuration.
   * @return a new path representing the same location as the input 'path',
   * but with a fully-qualified URI.
   */
  public static Path qualify(Path path, Configuration conf)
      throws IOException {
    if (null == path) {
      return null;
    }

    FileSystem fs = path.getFileSystem(conf);
    if (null == fs) {
      fs = FileSystem.get(conf);
    }
    return path.makeQualified(fs);
  }
}
