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
package org.apache.sqoop.connector.hdfs.configuration;

import org.apache.sqoop.model.ConfigClass;
import org.apache.sqoop.model.Input;
import org.apache.sqoop.model.Validator;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.validators.AbstractValidator;
import org.apache.sqoop.validation.validators.NotEmpty;

/**
 */
@ConfigClass(validators = { @Validator(ToJobConfig.ToJobConfigValidator.class)})
public class ToJobConfig {

  @Input(size = 255) public Boolean overrideNullValue;

  @Input(size = 255) public String nullValue;

  @Input public ToFormat outputFormat;

  @Input public ToCompression compression;

  @Input(size = 255) public String customCompression;

  @Input(size = 255, validators = { @Validator(NotEmpty.class)}) public String outputDirectory;

  @Input public Boolean appendMode;

  public static class ToJobConfigValidator extends AbstractValidator<ToJobConfig> {
    @Override
    public void validate(ToJobConfig conf) {
      if(conf.customCompression != null &&
         conf.customCompression.trim().length() > 0 &&
         conf.compression != ToCompression.CUSTOM) {
        addMessage(Status.ERROR, "Custom compression codec should be blank as " + conf.compression + " is being used");
      }

      if(conf.compression == ToCompression.CUSTOM &&
         (conf.customCompression == null || conf.customCompression.trim().length() == 0)) {
        addMessage(Status.ERROR, "Custom compression field is blank.");
      }
    }
  }
}
