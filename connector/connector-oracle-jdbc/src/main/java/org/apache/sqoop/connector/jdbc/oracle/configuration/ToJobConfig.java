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
package org.apache.sqoop.connector.jdbc.oracle.configuration;

import java.util.List;

import org.apache.sqoop.connector.jdbc.oracle.util.OracleUtilities;
import org.apache.sqoop.model.ConfigClass;
import org.apache.sqoop.model.Input;
import org.apache.sqoop.model.Validator;
import org.apache.sqoop.validation.validators.NotEmpty;

/**
 *
 */
@ConfigClass
public class ToJobConfig {

  @Input(size = 2000, validators = { @Validator(NotEmpty.class)})
  public String tableName;

  @Input
  public List<String> columns;

  @Input(size = 2000)
  public String templateTable;

  @Input
  public Boolean partitioned;

  @Input
  public Boolean nologging;

  @Input
  public List<String> updateKey;

  @Input
  public Boolean updateMerge;

  @Input
  public Boolean dropTableIfExists;

  @Input(size = 2000)
  public String storageClause;

  @Input(size = 2000)
  public String temporaryStorageClause;

  @Input
  public OracleUtilities.AppendValuesHintUsage appendValuesHint;

  @Input
  public Boolean parallel;

}
