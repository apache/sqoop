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
package org.apache.sqoop.connector.kite.configuration;

import com.google.common.base.Strings;
import org.apache.sqoop.model.ConfigClass;
import org.apache.sqoop.model.Input;
import org.apache.sqoop.model.Validator;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.validators.AbstractValidator;
import org.apache.sqoop.validation.validators.HostAndPortValidator;

@ConfigClass(validators = {@Validator(LinkConfig.ConfigValidator.class)})
public class LinkConfig {

  @Input(size = 255)
  public String authority;

  public static class ConfigValidator extends AbstractValidator<LinkConfig> {

    @Override
    public void validate(LinkConfig config) {
      // TODO: There is no way to declare it as optional (SQOOP-1643), we cannot validate it directly using HostAndPortValidator.
      if (!Strings.isNullOrEmpty(config.authority)) {
        HostAndPortValidator validator = new HostAndPortValidator();
        validator.validate(config.authority);
        if (!validator.getStatus().equals(Status.OK)) {
          addMessage(validator.getStatus(), getMessages().toString());
        }
      }
    }
  }

}