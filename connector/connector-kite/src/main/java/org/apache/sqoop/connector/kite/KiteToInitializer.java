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
package org.apache.sqoop.connector.kite;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.common.JarUtil;
import org.apache.sqoop.connector.kite.configuration.LinkConfiguration;
import org.apache.sqoop.connector.kite.configuration.ToJobConfiguration;
import org.apache.sqoop.job.etl.Initializer;
import org.apache.sqoop.job.etl.InitializerContext;
import org.apache.sqoop.schema.Schema;

import java.util.List;
import java.util.regex.Pattern;

/**
 * This class allows connector to define initialization work for execution.
 *
 * It will check whether dataset exists in destination already.
 */
public class KiteToInitializer extends Initializer<LinkConfiguration,
    ToJobConfiguration> {

  private static final Logger LOG = Logger.getLogger(KiteToInitializer.class);

  // Minimal dependencies for the MR job
  private static final Pattern[] JAR_NAME_PATTERNS = {
      Pattern.compile("/kite-"),
      Pattern.compile("/jackson-(annotations|core|databind)-\\d+"),
  };

  @Override
  public void initialize(InitializerContext context,
      LinkConfiguration linkConfig, ToJobConfiguration jobConfig) {
    if (KiteDatasetExecutor.datasetExists(jobConfig.toJobConfig.uri)) {
      LOG.error("Overwrite an existing dataset is not expected in new create mode.");
      throw new SqoopException(KiteConnectorError.GENERIC_KITE_CONNECTOR_0001);
    }
  }

  @Override
  public List<String> getJars(InitializerContext context,
      LinkConfiguration linkConfig, ToJobConfiguration jobConfig) {
    List<String> jars = super.getJars(context, linkConfig, jobConfig);
    jars.addAll(JarUtil.getMatchedJars(JAR_NAME_PATTERNS));
    return jars;
  }

  @Override
  public Schema getSchema(InitializerContext context,
      LinkConfiguration linkConfig, ToJobConfiguration jobConfig) {
    // TO-direction does not have a schema, so return a dummy schema.
    return new Schema("Kite dataset");
  }

}