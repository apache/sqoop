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

package org.apache.sqoop.metastore;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.tool.SqoopTool;

/**
 * Container for all job data that should be stored to a
 * permanent resource.
 */
public class JobData {
  private SqoopOptions opts;
  private SqoopTool tool;

  public JobData() {
  }

  public JobData(SqoopOptions options, SqoopTool sqoopTool) {
    this.opts = options;
    this.tool = sqoopTool;
  }

  /**
   * Gets the SqoopOptions.
   */
  public SqoopOptions getSqoopOptions() {
    return this.opts;
  }

  /**
   * Gets the SqoopTool.
   */
  public SqoopTool getSqoopTool() {
    return this.tool;
  }

  /**
   * Sets the SqoopOptions.
   */
  public void setSqoopOptions(SqoopOptions options) {
    this.opts = options;
  }

  /**
   * Sets the SqoopTool.
   */
  public void setSqoopTool(SqoopTool sqoopTool) {
    this.tool = sqoopTool;
  }

}

