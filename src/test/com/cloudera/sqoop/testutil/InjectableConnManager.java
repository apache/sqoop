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

package com.cloudera.sqoop.testutil;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.HsqldbManager;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.mapreduce.ImportJobBase;
import com.cloudera.sqoop.util.ImportException;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A ConnManager that uses dependency injection to control "import jobs"
 * that are mocked for testing.
 */
public class InjectableConnManager extends HsqldbManager {

  // The Configuration is used to control the injected classes.
  public static final String MAPPER_KEY = "sqoop.inject.mapper.class";
  public static final String INPUT_FORMAT_KEY =
      "sqoop.inject.input.format.class";
  public static final String OUTPUT_FORMAT_KEY =
      "sqoop.inject.output.format.class";
  public static final String IMPORT_JOB_KEY =
      "sqoop.inject.import.job.class";

  public InjectableConnManager(final SqoopOptions options) {
    super(options);
  }

  /**
   * Allow the user to inject custom mapper, input, and output formats
   * into the importTable() process.
   */
  @Override
  @SuppressWarnings("unchecked")
  public void importTable(ImportJobContext context)
      throws IOException, ImportException {

    SqoopOptions options = context.getOptions();
    Configuration conf = options.getConf();

    Class<? extends Mapper> mapperClass = (Class<? extends Mapper>)
        conf.getClass(MAPPER_KEY, Mapper.class);
    Class<? extends InputFormat> ifClass = (Class<? extends InputFormat>)
        conf.getClass(INPUT_FORMAT_KEY, TextInputFormat.class);
    Class<? extends OutputFormat> ofClass = (Class<? extends OutputFormat>)
        conf.getClass(OUTPUT_FORMAT_KEY, TextOutputFormat.class);

    Class<? extends ImportJobBase> jobClass = (Class<? extends ImportJobBase>)
        conf.getClass(IMPORT_JOB_KEY, ImportJobBase.class);

    String tableName = context.getTableName();

    // Instantiate the user's chosen ImportJobBase instance.
    ImportJobBase importJob = ReflectionUtils.newInstance(jobClass, conf);

    // And configure the dependencies to inject
    importJob.setOptions(options);
    importJob.setMapperClass(mapperClass);
    importJob.setInputFormatClass(ifClass);
    importJob.setOutputFormatClass(ofClass);

    importJob.runImport(tableName, context.getJarFile(),
        getSplitColumn(options, tableName), conf);
  }
}

