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
import org.apache.sqoop.connector.common.FileFormat;
import org.apache.sqoop.connector.kite.configuration.ConfigUtil;
import org.apache.sqoop.connector.kite.configuration.LinkConfiguration;
import org.apache.sqoop.connector.kite.configuration.ToJobConfiguration;
import org.apache.sqoop.job.etl.Initializer;
import org.apache.sqoop.job.etl.InitializerContext;
import org.apache.sqoop.schema.NullSchema;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.utils.ClassUtils;

import java.util.Set;

/**
 * This class allows connector to define initialization work for execution.
 *
 * It will check whether dataset exists in destination already.
 */
public class KiteToInitializer extends Initializer<LinkConfiguration,
    ToJobConfiguration> {

  private static final Logger LOG = Logger.getLogger(KiteToInitializer.class);

  @Override
  public void initialize(InitializerContext context,
      LinkConfiguration linkConfig, ToJobConfiguration toJobConfig) {
    String uri = ConfigUtil.buildDatasetUri(
        linkConfig.linkConfig, toJobConfig.toJobConfig);
    if (KiteDatasetExecutor.datasetExists(uri)) {
      LOG.error("Overwrite an existing dataset is not expected in new create mode.");
      throw new SqoopException(KiteConnectorError.GENERIC_KITE_CONNECTOR_0001);
    }
  }

  @Override
  public Set<String> getJars(InitializerContext context,
      LinkConfiguration linkConfig, ToJobConfiguration toJobConfig) {
    Set<String> jars = super.getJars(context, linkConfig, toJobConfig);
    jars.add(ClassUtils.jarForClass("org.kitesdk.data.Formats"));
    jars.add(ClassUtils.jarForClass("com.fasterxml.jackson.databind.JsonNode"));
    jars.add(ClassUtils.jarForClass("com.fasterxml.jackson.core.TreeNode"));
    if (FileFormat.CSV.equals(toJobConfig.toJobConfig.fileFormat)) {
      jars.add(ClassUtils.jarForClass("au.com.bytecode.opencsv.CSVWriter"));
    }
    return jars;
  }

  @Override
  public Schema getSchema(InitializerContext context,
      LinkConfiguration linkConfig, ToJobConfiguration toJobConfig) {
    return NullSchema.getInstance();
  }

}