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
package org.apache.sqoop.connector.jdbc.configuration;

import org.apache.sqoop.model.ConfigClass;
import org.apache.sqoop.model.Input;
import org.apache.sqoop.model.InputEditable;
import org.apache.sqoop.model.Validator;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.validators.AbstractValidator;

/**
 */
@ConfigClass(validators = {@Validator(IncrementalRead.ConfigValidator.class)})
public class IncrementalRead {
  @Input(size = 50)
  public String checkColumn;

  @Input(editable = InputEditable.ANY)
  public String lastValue;

  public static class ConfigValidator extends AbstractValidator<IncrementalRead> {
    @Override
    public void validate(IncrementalRead conf) {
      if(conf.checkColumn != null && conf.lastValue == null) {
        addMessage(Status.ERROR, "Last value is required during incremental read");
      }

      if(conf.checkColumn == null && conf.lastValue != null) {
        addMessage(Status.ERROR, "Last value can't be filled without check column.");
      }
    }
  }
}
