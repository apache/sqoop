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

package org.apache.sqoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.sqoop.util.LoggingUtils;

import java.io.IOException;

/**
 * Base sqoop mapper class that is convenient place for common functionality.
 * Other specific mappers are highly encouraged to inherit from this class.
 */
public abstract class SqoopMapper<KI, VI, KO, VO>
  extends Mapper<KI, VI, KO, VO> {

  @Override
  protected void setup(Context context)
    throws IOException, InterruptedException {
    super.setup(context);

    Configuration configuration = context.getConfiguration();

    // Propagate verbose flag if needed
    if (configuration.getBoolean(JobBase.PROPERTY_VERBOSE, false)) {
      LoggingUtils.setDebugLevel();
    }
  }
}
