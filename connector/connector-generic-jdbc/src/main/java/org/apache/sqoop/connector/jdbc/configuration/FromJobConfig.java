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

import org.apache.sqoop.connector.jdbc.GenericJdbcConnectorConstants;
import org.apache.sqoop.model.ConfigClass;
import org.apache.sqoop.model.Input;
import org.apache.sqoop.model.Validator;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.validators.AbstractValidator;
import org.apache.sqoop.validation.validators.NullOrContains;

/**
 *
 */
@ConfigClass(validators = { @Validator(FromJobConfig.ConfigValidator.class) })
public class FromJobConfig {
  @Input(size = 50)
  public String schemaName;

  @Input(size = 50)
  public String tableName;

  @Input(size = 2000, validators = { @Validator(value = NullOrContains.class, strArg = GenericJdbcConnectorConstants.SQL_CONDITIONS_TOKEN) })
  public String sql;

  @Input(size = 50)
  public String columns;

  @Input(size = 50)
  public String partitionColumn;

  @Input
  public Boolean allowNullValueInPartitionColumn;

  @Input(size = 50)
  public String boundaryQuery;

  public static class ConfigValidator extends AbstractValidator<FromJobConfig> {
    @Override
    public void validate(FromJobConfig config) {
      if (config.tableName == null && config.sql == null) {
        addMessage(Status.ERROR, "Either table name or SQL must be specified");
      }
      if (config.tableName != null && config.sql != null) {
        addMessage(Status.ERROR, "Both table name and SQL cannot be specified");
      }
      if (config.schemaName != null && config.sql != null) {
        addMessage(Status.ERROR, "Both schema name and SQL cannot be specified");
      }
      if (config.sql != null && config.partitionColumn == null) {
        addMessage(Status.ERROR, "Partition column is required on query based import.");
      }
    }
  }
}
