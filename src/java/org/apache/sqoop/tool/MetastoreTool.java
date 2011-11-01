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

package org.apache.sqoop.tool;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.SqoopOptions.InvalidOptionsException;
import com.cloudera.sqoop.cli.RelatedOptions;
import com.cloudera.sqoop.cli.ToolOptions;
import com.cloudera.sqoop.metastore.hsqldb.HsqldbMetaStore;

/**
 * Tool that runs a standalone Sqoop metastore.
 */
public class MetastoreTool extends com.cloudera.sqoop.tool.BaseSqoopTool {

  public static final Log LOG = LogFactory.getLog(
      MetastoreTool.class.getName());

  private HsqldbMetaStore metastore;

  // If set to true, shut an existing metastore down.
  private boolean shutdown = false;

  public MetastoreTool() {
    super("metastore");
  }

  @Override
  /** {@inheritDoc} */
  public int run(SqoopOptions options) {
    metastore = new HsqldbMetaStore(options.getConf());
    if (shutdown) {
      LOG.info("Shutting down metastore...");
      metastore.shutdown();
    } else {
      metastore.start();
      metastore.waitForServer();
      LOG.info("Server thread has quit.");
    }
    return 0;
  }

  @Override
  /** Configure the command-line arguments we expect to receive */
  public void configureOptions(ToolOptions toolOptions) {
    RelatedOptions opts = new RelatedOptions("metastore arguments");
    opts.addOption(OptionBuilder
        .withDescription("Cleanly shut down a running metastore")
        .withLongOpt(METASTORE_SHUTDOWN_ARG)
        .create());

    toolOptions.addUniqueOptions(opts);
  }

  @Override
  /** {@inheritDoc} */
  public void applyOptions(CommandLine in, SqoopOptions out)
      throws InvalidOptionsException {
    if (in.hasOption(METASTORE_SHUTDOWN_ARG)) {
      this.shutdown = true;
    }
  }

  @Override
  /** {@inheritDoc} */
  public void validateOptions(SqoopOptions options)
      throws InvalidOptionsException {
  }
}

