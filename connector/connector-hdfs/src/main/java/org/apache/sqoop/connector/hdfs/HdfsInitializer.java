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
package org.apache.sqoop.connector.hdfs;

import org.apache.sqoop.job.etl.Initializer;
import org.apache.sqoop.job.etl.InitializerContext;
import org.apache.sqoop.schema.Schema;


public class HdfsInitializer extends Initializer {
  /**
   * Initialize new submission based on given configuration properties. Any
   * needed temporary values might be saved to context object and they will be
   * promoted to all other part of the workflow automatically.
   *
   * @param context Initializer context object
   * @param linkConf       Connector's link configuration object
   * @param jobConf      Connector's job configuration object
   */
  @Override
  public void initialize(InitializerContext context, Object linkConf, Object jobConf) {

  }

  @Override
  public Schema getSchema(InitializerContext context, Object linkConf, Object jobConf) {
    return new Schema("HDFS file");
  }
}
