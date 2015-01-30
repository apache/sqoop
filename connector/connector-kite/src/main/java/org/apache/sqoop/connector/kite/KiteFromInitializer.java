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
import org.apache.sqoop.connector.kite.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.kite.configuration.LinkConfiguration;
import org.apache.sqoop.connector.common.AvroDataTypeUtil;
import org.apache.sqoop.error.code.KiteConnectorError;
import org.apache.sqoop.job.etl.Initializer;
import org.apache.sqoop.job.etl.InitializerContext;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.utils.ClassUtils;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.Datasets;

import java.util.Set;

/**
 * This class allows connector to define initialization work for execution.
 */
public class KiteFromInitializer extends Initializer<LinkConfiguration,
    FromJobConfiguration> {

  private static final Logger LOG = Logger.getLogger(KiteFromInitializer.class);

  @Override
  public void initialize(InitializerContext context,
      LinkConfiguration linkConfig, FromJobConfiguration fromJobConfig) {
    if (!Datasets.exists(fromJobConfig.fromJobConfig.uri)) {
      LOG.error("Dataset does not exist");
      throw new SqoopException(KiteConnectorError.GENERIC_KITE_CONNECTOR_0002);
    }
  }
  @Override
  public Set<String> getJars(InitializerContext context,
      LinkConfiguration linkConfig, FromJobConfiguration fromJobConfig) {
    Set<String> jars = super.getJars(context, linkConfig, fromJobConfig);
    jars.add(ClassUtils.jarForClass("org.kitesdk.data.Datasets"));
    jars.add(ClassUtils.jarForClass("com.fasterxml.jackson.databind.JsonNode"));
    jars.add(ClassUtils.jarForClass("com.fasterxml.jackson.core.TreeNode"));
    return jars;
  }

  @Override
  public Schema getSchema(InitializerContext context,
      LinkConfiguration linkConfig, FromJobConfiguration fromJobConfig) {
    Dataset dataset = Datasets.load(fromJobConfig.fromJobConfig.uri);
    org.apache.avro.Schema avroSchema = dataset.getDescriptor().getSchema();
    return AvroDataTypeUtil.createSqoopSchema(avroSchema);
  }

}