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
package org.apache.sqoop.shell.utils;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.model.MJob;

/**
 * Automatically create dynamic options for jobs.
 */
@SuppressWarnings("serial")
public class JobDynamicConfigOptions extends DynamicConfigOptions<MJob> {

  @SuppressWarnings("static-access")
  @Override
  public void prepareOptions(MJob job) {
    this.addOption(OptionBuilder
                  .withLongOpt("name")
                  .hasArg()
                  .create());
    for (Option option : ConfigOptions.getConfigsOptions("from", job.getFromJobConfig().getConfigs())) {
      this.addOption(option);
    }
    for (Option option : ConfigOptions.getConfigsOptions("driver", job.getDriverConfig().getConfigs())) {
      this.addOption(option);
    }
    for (Option option : ConfigOptions.getConfigsOptions("to", job.getToJobConfig().getConfigs())) {
      this.addOption(option);
    }
  }
}
