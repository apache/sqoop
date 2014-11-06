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
package com.cloudera.sqoop.lib;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Contains a set of methods which can read db columns from a ResultSet into
 * Java types, and do serialization of these types to/from DataInput/DataOutput
 * for use with Hadoop's Writable implementation. This supports null values
 * for all types.
 *
 * This is a singleton instance class; only one may exist at a time.
 * However, its lifetime is limited to the current TaskInputOutputContext's
 * life.
 *
 * @deprecated use org.apache.sqoop.lib.LargeObjectLoader instead.
 * @see org.apache.sqoop.lib.LargeObjectLoader
 */
public class LargeObjectLoader extends org.apache.sqoop.lib.LargeObjectLoader {

  // Spill to external storage for BLOB/CLOB objects > 16 MB.
  public static final long DEFAULT_MAX_LOB_LENGTH =
      org.apache.sqoop.lib.LargeObjectLoader.DEFAULT_MAX_LOB_LENGTH;

  public static final String MAX_INLINE_LOB_LEN_KEY =
      org.apache.sqoop.lib.LargeObjectLoader.MAX_INLINE_LOB_LEN_KEY;

  /**
   * Create a new LargeObjectLoader.
   * @param conf the Configuration to use
   */
  public LargeObjectLoader(Configuration conf, Path workPath)
      throws IOException {
    super(conf, workPath);
  }
}
