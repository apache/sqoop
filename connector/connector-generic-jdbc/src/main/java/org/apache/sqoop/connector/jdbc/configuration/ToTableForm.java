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

import org.apache.sqoop.model.FormClass;
import org.apache.sqoop.model.Input;
import org.apache.sqoop.model.Validator;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.validators.AbstractValidator;

/**
 *
 */
@FormClass(validators = {@Validator(ToTableForm.FormValidator.class)})
public class ToTableForm {
  @Input(size = 50)   public String schemaName;
  @Input(size = 2000) public String tableName;
  @Input(size = 50)   public String sql;
  @Input(size = 50)   public String columns;
  @Input(size = 2000) public String stageTableName;
  @Input              public Boolean clearStageTable;

  public static class FormValidator extends AbstractValidator<ToTableForm> {
    @Override
    public void validate(ToTableForm form) {
      if(form.tableName == null && form.sql == null) {
        addMessage(Status.UNACCEPTABLE, "Either fromTable name or SQL must be specified");
      }
      if(form.tableName != null && form.sql != null) {
        addMessage(Status.UNACCEPTABLE, "Both fromTable name and SQL cannot be specified");
      }
      if(form.tableName == null && form.stageTableName != null) {
        addMessage(Status.UNACCEPTABLE, "Stage fromTable name cannot be specified without specifying fromTable name");
      }
      if(form.stageTableName == null && form.clearStageTable != null) {
        addMessage(Status.UNACCEPTABLE, "Clear stage fromTable cannot be specified without specifying name of the stage fromTable.");
      }
    }
  }
}
