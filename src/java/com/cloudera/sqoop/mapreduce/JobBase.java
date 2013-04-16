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
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.sqoop.config.ConfigurationHelper;
import org.apache.sqoop.manager.ConnManager;
import org.apache.sqoop.validation.ValidationContext;
import org.apache.sqoop.validation.ValidationException;
import org.apache.sqoop.validation.ValidationFailureHandler;
import org.apache.sqoop.validation.ValidationThreshold;
import org.apache.sqoop.validation.Validator;

import com.cloudera.sqoop.SqoopOptions;

/**
 * @deprecated Moving to use org.apache.sqoop namespace.
 */
@Deprecated
public class JobBase
    extends org.apache.sqoop.mapreduce.JobBase {

  public JobBase() {
    super();
  }

  public JobBase(final SqoopOptions opts) {
    super(opts);
  }

  public JobBase(final SqoopOptions opts,
      final Class<? extends Mapper> mapperClass,
      final Class<? extends InputFormat> inputFormatClass,
      final Class<? extends OutputFormat> outputFormatClass) {
    super(opts, mapperClass, inputFormatClass, outputFormatClass);
  }

  protected long getRowCountFromDB(ConnManager connManager, String tableName)
    throws SQLException {
    return connManager.getTableRowCount(tableName);
  }

  protected long getRowCountFromHadoop(Job job)
    throws IOException, InterruptedException {
    return ConfigurationHelper.getNumMapOutputRecords(job);
  }

  protected void doValidate(SqoopOptions options, Configuration conf,
                            ValidationContext validationContext)
    throws ValidationException {
    Validator validator = (Validator) ReflectionUtils.newInstance(
        options.getValidatorClass(), conf);
    ValidationThreshold threshold = (ValidationThreshold)
        ReflectionUtils.newInstance(options.getValidationThresholdClass(),
          conf);
    ValidationFailureHandler failureHandler = (ValidationFailureHandler)
        ReflectionUtils.newInstance(options.getValidationFailureHandlerClass(),
          conf);

    StringBuilder sb = new StringBuilder();
    sb.append("Validating the integrity of the import using the "
      + "following configuration\n");
    sb.append("\tValidator : ").append(validator.getClass().getName())
      .append('\n');
    sb.append("\tThreshold Specifier : ")
      .append(threshold.getClass().getName()).append('\n');
    sb.append("\tFailure Handler : ")
      .append(failureHandler.getClass().getName()).append('\n');
    LOG.info(sb.toString());
    validator.validate(validationContext, threshold, failureHandler);
  }
}
