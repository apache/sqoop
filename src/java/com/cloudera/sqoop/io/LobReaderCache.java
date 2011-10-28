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
package com.cloudera.sqoop.io;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * A cache of open LobFile.Reader objects.
 * This maps from filenames to the open Reader, if any.  This uses the
 * Singleton pattern. While nothing prevents multiple LobReaderCache
 * instances, it is most useful to have a single global cache. This cache is
 * internally synchronized; only one thread can insert or retrieve a reader
 * from the cache at a time.
 *
 * @deprecated use org.apache.sqoop.io.LobReaderCache instead.
 * @see org.apache.sqoop.io.LobReaderCache
 */
public final class LobReaderCache extends org.apache.sqoop.io.LobReaderCache {

  public static final Log LOG = org.apache.sqoop.io.LobReaderCache.LOG;

  private static final LobReaderCache CACHE;
  static {
    CACHE = new LobReaderCache();
  }

  /**
   * @return the singleton LobReaderCache instance.
   */
  public static LobReaderCache getCache() {
    return CACHE;
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
    return org.apache.sqoop.io.LobReaderCache.qualify(path, conf);
  }
}

