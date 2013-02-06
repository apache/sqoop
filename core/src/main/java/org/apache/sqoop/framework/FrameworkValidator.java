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
package org.apache.sqoop.framework;

import org.apache.sqoop.framework.configuration.ConnectionConfiguration;
import org.apache.sqoop.framework.configuration.ExportJobConfiguration;
import org.apache.sqoop.framework.configuration.ImportJobConfiguration;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.Validation;
import org.apache.sqoop.validation.Validator;

/**
 * Validate framework configuration objects
 */
public class FrameworkValidator extends Validator {

  @Override
  public Validation validateConnection(Object connectionConfiguration) {
    Validation validation = new Validation(ConnectionConfiguration.class);
    // No validation on connection object
    return validation;
  }


  @Override
  public Validation validateJob(MJob.Type type, Object jobConfiguration) {
    switch(type) {
      case IMPORT:
        return validateImportJob(jobConfiguration);
      case EXPORT:
        return validateExportJob(jobConfiguration);
      default:
        return super.validateJob(type, jobConfiguration);
    }
  }

  private Validation validateExportJob(Object jobConfiguration) {
    Validation validation = new Validation(ExportJobConfiguration.class);
    ExportJobConfiguration configuration = (ExportJobConfiguration)jobConfiguration;

    if(configuration.input.inputDirectory == null || configuration.input.inputDirectory.isEmpty()) {
      validation.addMessage(Status.UNACCEPTABLE, "input", "inputDirectory", "Input directory is empty");
    }

    return validation;
  }

  private Validation validateImportJob(Object jobConfiguration) {
    Validation validation = new Validation(ImportJobConfiguration.class);
    ImportJobConfiguration configuration = (ImportJobConfiguration)jobConfiguration;

    if(configuration.output.outputDirectory == null || configuration.output.outputDirectory.isEmpty()) {
      validation.addMessage(Status.UNACCEPTABLE, "output", "outputDirectory", "Input directory is empty");
    }

    return validation;
  }
}
