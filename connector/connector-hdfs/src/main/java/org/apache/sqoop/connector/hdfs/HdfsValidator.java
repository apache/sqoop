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
import org.apache.sqoop.validation.Validation;
import org.apache.sqoop.validation.Validator;

/**
 * Validate framework configuration objects
 */
public class HdfsValidator extends Validator {

  @Override
  public Validation validateConnection(Object connectionConfiguration) {
    Validation validation = new Validation(ConnectionConfiguration.class);
    // No validation on connection object
    return validation;
  }


  @Override
  public Validation validateJob(Object jobConfiguration) {
    //TODO: I'm pretty sure this needs to call either validateExportJob or validateImportJob, depending on context
    return super.validateJob(jobConfiguration);
  }

  private Validation validateExportJob(Object jobConfiguration) {
    Validation validation = new Validation(FromJobConfiguration.class);
    FromJobConfiguration configuration = (FromJobConfiguration)jobConfiguration;

    validateInputForm(validation, configuration.input);


    return validation;
  }

  private Validation validateImportJob(Object jobConfiguration) {
    Validation validation = new Validation(ToJobConfiguration.class);
    ToJobConfiguration configuration = (ToJobConfiguration)jobConfiguration;

    validateOutputForm(validation, configuration.output);

    return validation;
  }

  private void validateInputForm(Validation validation, InputForm input) {
    if(input.inputDirectory == null || input.inputDirectory.isEmpty()) {
      validation.addMessage(Status.UNACCEPTABLE, "input", "inputDirectory", "Input directory is empty");
    }
  }

  private void validateOutputForm(Validation validation, OutputForm output) {
    if(output.outputDirectory == null || output.outputDirectory.isEmpty()) {
      validation.addMessage(Status.UNACCEPTABLE, "output", "outputDirectory", "Output directory is empty");
    }
    if(output.customCompression != null &&
      output.customCompression.trim().length() > 0  &&
      output.compression != OutputCompression.CUSTOM) {
      validation.addMessage(Status.UNACCEPTABLE, "output", "compression",
        "custom compression should be blank as " + output.compression + " is being used.");
    }
    if(output.compression == OutputCompression.CUSTOM &&
      (output.customCompression == null ||
        output.customCompression.trim().length() == 0)
      ) {
      validation.addMessage(Status.UNACCEPTABLE, "output", "compression",
        "custom compression is blank.");
    }
  }


}
