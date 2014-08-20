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
package org.apache.sqoop.connector.hdfs.hdfsWriter;

import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.sqoop.connector.hdfs.HdfsConstants;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class HdfsTextWriter extends GenericHdfsWriter {

  private BufferedWriter filewriter;

  @Override
  public void initialize(Path filepath, Configuration conf, CompressionCodec codec) throws IOException {
    FileSystem fs = filepath.getFileSystem(conf);

    DataOutputStream filestream = fs.create(filepath, false);
    if (codec != null) {
      filewriter = new BufferedWriter(new OutputStreamWriter(
              codec.createOutputStream(filestream, codec.createCompressor()),
              Charsets.UTF_8));
    } else {
      filewriter = new BufferedWriter(new OutputStreamWriter(
              filestream, Charsets.UTF_8));
    }
  }

  @Override
  public void write(String csv) throws IOException {
    filewriter.write(csv + HdfsConstants.DEFAULT_RECORD_DELIMITER);

  }

  @Override
  public void destroy() throws IOException {
    filewriter.close();
  }
}
