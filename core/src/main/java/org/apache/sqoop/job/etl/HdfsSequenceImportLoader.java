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
package org.apache.sqoop.job.etl;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.core.CoreError;
import org.apache.sqoop.job.JobConstants;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.io.Data;
import org.apache.sqoop.job.io.DataReader;
import org.apache.sqoop.utils.ClassLoadingUtils;

public class HdfsSequenceImportLoader extends Loader {

  public static final String extension = ".seq";

  private final char fieldDelimiter;
  private final char recordDelimiter;

  public HdfsSequenceImportLoader() {
    fieldDelimiter = Data.DEFAULT_FIELD_DELIMITER;
    recordDelimiter = Data.DEFAULT_RECORD_DELIMITER;
  }

  @Override
  public void run(Context context, DataReader reader) {
    Configuration conf = ((EtlContext)context).getConfiguration();
    String filename =
        context.getString(JobConstants.JOB_MR_OUTPUT_FILE);
    String codecname = context.getString(JobConstants.JOB_MR_OUTPUT_CODEC);

    CompressionCodec codec = null;
    if (codecname != null) {
      Class<?> clz = ClassLoadingUtils.loadClass(codecname);
      if (clz == null) {
        throw new SqoopException(CoreError.CORE_0009, codecname);
      }

      try {
        codec = (CompressionCodec) clz.newInstance();
        if (codec instanceof Configurable) {
          ((Configurable) codec).setConf(conf);
        }
      } catch (Exception e) {
        throw new SqoopException(CoreError.CORE_0010, codecname, e);
      }
    }

    filename += extension;

    try {
      Path filepath = new Path(filename);
      SequenceFile.Writer filewriter;
      if (codecname != null) {
        filewriter = SequenceFile.createWriter(conf,
            SequenceFile.Writer.file(filepath),
            SequenceFile.Writer.keyClass(Text.class),
            SequenceFile.Writer.valueClass(NullWritable.class),
            SequenceFile.Writer.compression(CompressionType.BLOCK, codec));
      } else {
        filewriter = SequenceFile.createWriter(conf,
          SequenceFile.Writer.file(filepath),
          SequenceFile.Writer.keyClass(Text.class),
          SequenceFile.Writer.valueClass(NullWritable.class),
          SequenceFile.Writer.compression(CompressionType.NONE));
      }

      Object record;
      Text text = new Text();
      while ((record = reader.readRecord()) != null) {
        text.set(Data.format(record, fieldDelimiter, recordDelimiter));
        filewriter.append(text, NullWritable.get());
      }
      filewriter.close();

    } catch (IOException e) {
      throw new SqoopException(CoreError.CORE_0018, e);
    }

  }

}
