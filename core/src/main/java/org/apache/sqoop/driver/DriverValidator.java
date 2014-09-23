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
package org.apache.sqoop.driver;

import org.apache.sqoop.driver.configuration.LinkConfiguration;
import org.apache.sqoop.driver.configuration.JobConfiguration;
import org.apache.sqoop.driver.configuration.ThrottlingForm;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.Validation;
import org.apache.sqoop.validation.Validator;

public class DriverValidator extends Validator {
  @Override
  public Validation validateLink(Object linkConfiguration) {
    Validation validation = new Validation(LinkConfiguration.class);
    // No validation on link object
    return validation;
  }

  @Override
  public Validation validateJob(Object jobConfiguration) {
    Validation validation = new Validation(JobConfiguration.class);
    JobConfiguration conf = (JobConfiguration)jobConfiguration;
    validateThrottlingForm(validation,conf.throttling);

    return validation;
  };

  private void validateThrottlingForm(Validation validation, ThrottlingForm throttling) {
    if(throttling.extractors != null && throttling.extractors < 1) {
      validation.addMessage(Status.UNACCEPTABLE, "throttling", "extractors", "You need to specify more than one extractor");
    }

    if(throttling.loaders != null && throttling.loaders < 1) {
      validation.addMessage(Status.UNACCEPTABLE, "throttling", "loaders", "You need to specify more than one loader");
    }
  }

}
