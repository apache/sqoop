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
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import com.cloudera.sqoop.manager.ExportJobContext;

/**
 * @deprecated Moving to use org.apache.sqoop namespace.
 */
public class JdbcUpdateExportJob
    extends org.apache.sqoop.mapreduce.JdbcUpdateExportJob {

  public JdbcUpdateExportJob(final ExportJobContext context)
      throws IOException {
    super(context);
  }

  public JdbcUpdateExportJob(final ExportJobContext ctxt,
      final Class<? extends Mapper> mapperClass,
      final Class<? extends InputFormat> inputFormatClass,
      final Class<? extends OutputFormat> outputFormatClass) {
    super(ctxt, mapperClass, inputFormatClass, outputFormatClass);
  }

}

