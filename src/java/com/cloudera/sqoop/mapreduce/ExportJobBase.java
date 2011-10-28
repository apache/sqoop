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

package com.cloudera.sqoop.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import com.cloudera.sqoop.manager.ExportJobContext;

/**
 * @deprecated Moving to use org.apache.sqoop namespace.
 */
public class ExportJobBase
    extends org.apache.sqoop.mapreduce.ExportJobBase {

  public static final String SQOOP_EXPORT_TABLE_CLASS_KEY =
      org.apache.sqoop.mapreduce.ExportJobBase.SQOOP_EXPORT_TABLE_CLASS_KEY;

  public static final String SQOOP_EXPORT_UPDATE_COL_KEY =
      org.apache.sqoop.mapreduce.ExportJobBase.SQOOP_EXPORT_UPDATE_COL_KEY;

  public static final String EXPORT_MAP_TASKS_KEY =
      org.apache.sqoop.mapreduce.ExportJobBase.EXPORT_MAP_TASKS_KEY;

  public ExportJobBase(final ExportJobContext ctxt) {
    super(ctxt);
  }

  public ExportJobBase(final ExportJobContext ctxt,
      final Class<? extends Mapper> mapperClass,
      final Class<? extends InputFormat> inputFormatClass,
      final Class<? extends OutputFormat> outputFormatClass) {
    super(ctxt, mapperClass, inputFormatClass, outputFormatClass);
  }

  public static boolean isSequenceFiles(Configuration conf, Path p)
      throws IOException {
    return org.apache.sqoop.mapreduce.ExportJobBase.isSequenceFiles(conf, p);
  }

  public static FileType getFileType(Configuration conf, Path p)
      throws IOException {
    return org.apache.sqoop.mapreduce.ExportJobBase.getFileType(conf, p);
  }

}
