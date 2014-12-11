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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.UUID;

public class TestHdfsBase {

  protected HdfsPartition createPartition(Path[] paths) throws IOException {
    long[] offsets = new long[paths.length];
    long[] lengths = new long[paths.length];
    String[] locations = new String[paths.length];
    FileSystem fs = FileSystem.get(new Configuration());

    for (int i = 0; i < offsets.length; ++i) {
      locations[i] = paths[i].getName();
      lengths[i] = fs.getFileStatus(paths[i]).getLen();
    }

    return new HdfsPartition(paths, offsets, lengths, locations);
  }

  protected String formatRow(String format, int index) {
    String row = format.replaceAll("\\%s", "'" + index + "'");
    row = row.replaceAll("\\%d", Integer.toString(index));
    return row.replaceAll("\\%f", Double.toString((double)index));
  }

  protected void createTextInput(String indir,
                                Class<? extends CompressionCodec> clz,
                                int numberOfFiles,
                                int numberOfRows,
                                String format)
      throws IOException, InstantiationException, IllegalAccessException {
    Configuration conf = new Configuration();

    CompressionCodec codec = null;
    String extension = "";
    if (clz != null) {
      codec = clz.newInstance();
      if (codec instanceof Configurable) {
        ((Configurable) codec).setConf(conf);
      }
      extension = codec.getDefaultExtension();
    }

    int index = 1;
    for (int fi = 0; fi < numberOfFiles; fi++) {
      String fileName = indir + "/" + UUID.randomUUID() + extension;
      OutputStream filestream = FileUtils.create(fileName);
      BufferedWriter filewriter;
      if (codec != null) {
        filewriter = new BufferedWriter(new OutputStreamWriter(
            codec.createOutputStream(filestream, codec.createCompressor()),
            "UTF-8"));
      } else {
        filewriter = new BufferedWriter(new OutputStreamWriter(
            filestream, "UTF-8"));
      }

      for (int ri = 0; ri < numberOfRows; ri++) {
        filewriter.write(formatRow(format, index) + HdfsConstants.DEFAULT_RECORD_DELIMITER);
        index++;
      }

      filewriter.close();
    }
  }

  protected void createTextInput(String indir,
                                 Class<? extends CompressionCodec> clz,
                                 int numberOfFiles,
                                 int numberOfRows)
      throws IOException, InstantiationException, IllegalAccessException {
    createTextInput(indir, clz, numberOfFiles, numberOfRows, "%d,%f,%s");
  }

  protected void createSequenceInput(String indir,
                                    Class<? extends CompressionCodec> clz,
                                    int numberOfFiles,
                                    int numberOfRows,
                                    String format)
      throws IOException, InstantiationException, IllegalAccessException {
    Configuration conf = new Configuration();

    CompressionCodec codec = null;
    if (clz != null) {
      codec = clz.newInstance();
      if (codec instanceof Configurable) {
        ((Configurable) codec).setConf(conf);
      }
    }

    int index = 1;
    for (int fi = 0; fi < numberOfFiles; fi++) {
      Path filepath = new Path(indir,UUID.randomUUID() + ".seq");
      SequenceFile.Writer filewriter;
      if (codec != null) {
        filewriter = SequenceFile.createWriter(filepath.getFileSystem(conf),
            conf, filepath, Text.class, NullWritable.class,
            SequenceFile.CompressionType.BLOCK, codec);
      } else {
        filewriter = SequenceFile.createWriter(filepath.getFileSystem(conf),
            conf, filepath, Text.class, NullWritable.class, SequenceFile.CompressionType.NONE);
      }

      Text text = new Text();
      for (int ri = 0; ri < numberOfRows; ri++) {
        text.set(formatRow(format, index));
        filewriter.append(text, NullWritable.get());
        index++;
      }

      filewriter.close();
    }
  }

  protected void createSequenceInput(String indir,
                                     Class<? extends CompressionCodec> clz,
                                     int numberOfFiles,
                                     int numberOfRows)
      throws IOException, InstantiationException, IllegalAccessException {
    createSequenceInput(indir, clz, numberOfFiles, numberOfRows, "%d,%f,%s");
  }
}
