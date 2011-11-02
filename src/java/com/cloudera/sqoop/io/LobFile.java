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
 * File format which stores large object records.
 * The format allows large objects to be read through individual InputStreams
 * to allow reading without full materialization of a single record.
 * Each record is assigned an id and can be accessed by id efficiently by
 * consulting an index at the end of the file.
 *
 * @deprecated use org.apache.sqoop.io.LobFile instead.
 * @see org.apache.sqoop.io.LobFile
 */
public final class LobFile {

  private LobFile() {
  }

  public static final Log LOG = org.apache.sqoop.io.LobFile.LOG;

  public static final int LATEST_LOB_VERSION =
      org.apache.sqoop.io.LobFile.LATEST_LOB_VERSION;

  // Must be in sync with org.apache.sqoop.io.LobFile.HEADER_ID_STR
  static final char [] HEADER_ID_STR =
      org.apache.sqoop.io.LobFile.HEADER_ID_STR;

  // Value for entryId to write to the beginning of an IndexSegment.
  static final long SEGMENT_HEADER_ID =
      org.apache.sqoop.io.LobFile.SEGMENT_HEADER_ID;

  // Value for entryId to write before the finale.
  static final long SEGMENT_OFFSET_ID =
      org.apache.sqoop.io.LobFile.SEGMENT_OFFSET_ID;

  // Value for entryID to write before the IndexTable
  static final long INDEX_TABLE_ID = org.apache.sqoop.io.LobFile.INDEX_TABLE_ID;

  /**
   * @deprecated use org.apache.sqoop.io.LobFile.Writer
   * @see org.apache.sqoop.io.LobFile.Writer
   */
  public abstract static class Writer
    extends org.apache.sqoop.io.LobFile.Writer {
  }

  /**
   * @deprecated use org.apache.sqoop.io.LobFile.Reader instead.
   * @see org.apache.sqoop.io.LobFile.Reader
   */
  public abstract static class Reader
    extends org.apache.sqoop.io.LobFile.Reader {
  }

  /**
   * Creates a LobFile Reader configured to read from the specified file.
   */
  public static Reader open(Path p, Configuration conf) throws IOException {
    return org.apache.sqoop.io.LobFile.open(p, conf);
  }

  /**
   * Creates a LobFile Writer configured for uncompressed binary data.
   * @param p the path to create.
   * @param conf the configuration to use to interact with the filesystem.
   */
  public static Writer create(Path p, Configuration conf) throws IOException {
    return org.apache.sqoop.io.LobFile.create(p, conf, false);
  }

  /**
   * Creates a LobFile Writer configured for uncompressed data.
   * @param p the path to create.
   * @param conf the configuration to use to interact with the filesystem.
   * @param isCharData true if this is for CLOBs, false for BLOBs.
   */
  public static Writer create(Path p, Configuration conf, boolean isCharData)
      throws IOException {
    return org.apache.sqoop.io.LobFile.create(p, conf, isCharData, null);
  }

  /**
   * Creates a LobFile Writer.
   * @param p the path to create.
   * @param conf the configuration to use to interact with the filesystem.
   * @param isCharData true if this is for CLOBs, false for BLOBs.
   * @param codec the compression codec to use (or null for none).
   */
  public static Writer create(Path p, Configuration conf, boolean isCharData,
      String codec) throws IOException {
    return org.apache.sqoop.io.LobFile.create(p, conf, isCharData, codec);
  }

  /**
   * Creates a LobFile Writer.
   * @param p the path to create.
   * @param conf the configuration to use to interact with the filesystem.
   * @param isCharData true if this is for CLOBs, false for BLOBs.
   * @param codec the compression codec to use (or null for none).
   * @param entriesPerSegment number of entries per index segment.
   */
  public static Writer create(Path p, Configuration conf, boolean isCharData,
      String codec, int entriesPerSegment)
      throws IOException {
    return org.apache.sqoop.io.LobFile.create(
        p, conf, isCharData, codec, entriesPerSegment);
  }
}

