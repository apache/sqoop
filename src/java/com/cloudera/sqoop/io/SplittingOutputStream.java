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
import org.apache.hadoop.io.compress.CompressionCodec;

/**
 * An output stream that writes to an underlying filesystem, opening
 * a new file after a specified number of bytes have been written to the
 * current one.
 *
 * @deprecated use org.apache.sqoop.io.SplittingOutputStream instead.
 * @see org.apache.sqoop.io.SplittingOutputStream
 */
public class SplittingOutputStream
    extends org.apache.sqoop.io.SplittingOutputStream {

  public static final Log LOG = org.apache.sqoop.io.SplittingOutputStream.LOG;

  public SplittingOutputStream(final Configuration conf, final Path destDir,
      final String filePrefix, final long cutoff, final CompressionCodec codec)
      throws IOException {
    super(conf, destDir, filePrefix, cutoff, codec);
  }
}
