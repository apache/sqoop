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
package org.apache.sqoop.schema;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

/**
 * The order of the matching options here indicates an order of preference
 * if it is possible to use both NAME and LOCATION matching options, we will prefer NAME
 *
 * NAME - match columns in FROM and TO schemas by column name. Data from column "hello"
 * will be written to a column named "hello". If TO schema doesn't have a column with
 * identical name, the column will be skipped.
 *
 * LOCATION - match columns in FROM and TO schemas by the column location.
 * Data from first column goes into first column in TO link.
 * If FROM link has more columns than TO, the extra columns will be skipped.
 *
 * USER_DEFINED - not implemented yet.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public enum SchemaMatchOption {
    NAME,
    LOCATION,
  //TODO: SQOOP-1546 - SQOOP2: Allow users to define their own schema matching
    USER_DEFINED
  }

