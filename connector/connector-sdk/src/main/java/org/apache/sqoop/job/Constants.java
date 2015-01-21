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
package org.apache.sqoop.job;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Constants {

  /**
   * All job related configuration is prefixed with this:
   * <tt>org.apache.sqoop.job.</tt>
   */
  public static final String PREFIX_CONFIG = "org.apache.sqoop.job.";

  public static final String JOB_ETL_NUMBER_PARTITIONS = PREFIX_CONFIG
      + "etl.number.partitions";

  public static final String JOB_ETL_FIELD_NAMES = PREFIX_CONFIG
      + "etl.field.names";

  public static final String JOB_ETL_OUTPUT_DIRECTORY = PREFIX_CONFIG
      + "etl.output.directory";

  public static final String JOB_ETL_INPUT_DIRECTORY = PREFIX_CONFIG
      + "etl.input.directory";

  protected Constants() {
    // Disable explicit object creation
  }

}
