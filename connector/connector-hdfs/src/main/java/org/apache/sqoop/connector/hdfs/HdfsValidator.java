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

import org.apache.sqoop.connector.hdfs.configuration.*;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.ConfigValidator;
import org.apache.sqoop.validation.Validator;

/**
 * Validate configuration objects
 */
public class HdfsValidator extends Validator {

  @Override
  public ConfigValidator validateConfigForJob(Object jobConfiguration) {
    return super.validateConfigForJob(jobConfiguration);
  }

  @SuppressWarnings("unused")
  private ConfigValidator validateFromJob(Object jobConfiguration) {
    ConfigValidator validation = new ConfigValidator(FromJobConfiguration.class);
    FromJobConfiguration configuration = (FromJobConfiguration)jobConfiguration;
    validateInputConfig(validation, configuration.fromJobConfig);
    return validation;
  }

  @SuppressWarnings("unused")
  private ConfigValidator validateToJob(Object jobConfiguration) {
    ConfigValidator validation = new ConfigValidator(ToJobConfiguration.class);
    ToJobConfiguration configuration = (ToJobConfiguration)jobConfiguration;
    validateOutputConfig(validation, configuration.toJobConfig);
    return validation;
  }

  private void validateInputConfig(ConfigValidator validation, FromJobConfig inputConfig) {
    if(inputConfig.inputDirectory == null || inputConfig.inputDirectory.isEmpty()) {
      validation.addMessage(Status.UNACCEPTABLE, "input", "inputDirectory", "Input directory is empty");
    }
  }

  private void validateOutputConfig(ConfigValidator validation, ToJobConfig outputConfig) {
    if(outputConfig.outputDirectory == null || outputConfig.outputDirectory.isEmpty()) {
      validation.addMessage(Status.UNACCEPTABLE, "output", "outputDirectory", "Output directory is empty");
    }
    if(outputConfig.customCompression != null &&
      outputConfig.customCompression.trim().length() > 0  &&
      outputConfig.compression != ToCompression.CUSTOM) {
      validation.addMessage(Status.UNACCEPTABLE, "output", "compression",
        "custom compression should be blank as " + outputConfig.compression + " is being used.");
    }
    if(outputConfig.compression == ToCompression.CUSTOM &&
      (outputConfig.customCompression == null ||
        outputConfig.customCompression.trim().length() == 0)
      ) {
      validation.addMessage(Status.UNACCEPTABLE, "output", "compression",
        "custom compression is blank.");
    }
  }
}
