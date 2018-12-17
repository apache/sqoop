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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.hadoop.security.SecurityUtils;
import org.apache.sqoop.connector.kite.configuration.ConfigUtil;
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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Set;

/**
 * This class allows connector to define initialization work for execution.
 */
public class KiteFromInitializer extends Initializer<LinkConfiguration,
    FromJobConfiguration> {

  private static final Logger LOG = Logger.getLogger(KiteFromInitializer.class);

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings({"SIC_INNER_SHOULD_BE_STATIC_ANON"})
  public void initialize(final InitializerContext context,
      final LinkConfiguration linkConfig, final FromJobConfiguration fromJobConfig) {
    final String uri = ConfigUtil.buildDatasetUri(
        linkConfig.linkConfig, fromJobConfig.fromJobConfig.uri);
    LOG.debug("Constructed dataset URI: " + uri);
    KiteUtils.addConfigDirToClasspath(linkConfig);
    try {
      SecurityUtils.createProxyUser(context).doAs(new PrivilegedExceptionAction<Void>() {
        public Void run() throws Exception {
          if (!Datasets.exists(uri)) {
            LOG.error("Dataset does not exist");
            throw new SqoopException(KiteConnectorError.GENERIC_KITE_CONNECTOR_0002);
          }

          if (ConfigUtil.isHdfsJob(fromJobConfig.fromJobConfig)) {
            // Generate delegation tokens if we are on secured cluster
            SecurityUtils.generateDelegationTokens(context.getContext(), new Path(ConfigUtil.removeDatasetPrefix(uri)), new Configuration());
          }
          return null;
        }
      });
    } catch (IOException | InterruptedException e) {
      throw new SqoopException(KiteConnectorError.GENERIC_KITE_CONNECTOR_0005, "Unexpected exception", e);
    }
  }

  @Override
  public Set<String> getJars(InitializerContext context,
      LinkConfiguration linkConfig, FromJobConfiguration fromJobConfig) {
    Set<String> jars = super.getJars(context, linkConfig, fromJobConfig);

    if (fromJobConfig.fromJobConfig.uri.startsWith("dataset:hive")) {
      // @TODO(Abe): Remove a deps that aren't used?
      jars.add(ClassUtils.jarForClass("org.apache.hadoop.hive.conf.HiveConf"));
      jars.add(ClassUtils.jarForClass("org.apache.hadoop.hive.ql.io.HiveOutputFormat"));
      jars.add(ClassUtils.jarForClass("org.apache.hadoop.hive.serde2.SerDe"));
      jars.add(ClassUtils.jarForClass("org.apache.hadoop.hive.metastore.Warehouse"));
      jars.add(ClassUtils.jarForClass("org.apache.hive.common.HiveCompat"));
      jars.add(ClassUtils.jarForClass("com.facebook.fb303.FacebookService"));
      jars.add(ClassUtils.jarForClass("org.datanucleus.query.compiler.JavaQueryCompiler"));
      jars.add(ClassUtils.jarForClass("org.datanucleus.query.typesafe.TypesafeSubquery"));
      jars.add(ClassUtils.jarForClass("org.datanucleus.store.rdbms.sql.SQLStatement"));
      jars.add(ClassUtils.jarForClass("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"));
    }
    return jars;
  }

  @SuppressWarnings("rawtypes")
  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings({"SIC_INNER_SHOULD_BE_STATIC_ANON"})
  public Schema getSchema(InitializerContext context,
      LinkConfiguration linkConfig, FromJobConfiguration fromJobConfig) {
    final String uri = ConfigUtil.buildDatasetUri(
        linkConfig.linkConfig, fromJobConfig.fromJobConfig.uri);
    Dataset dataset = null;
    try {
      dataset = SecurityUtils.createProxyUser(context).doAs(new PrivilegedExceptionAction<Dataset>() {
        public Dataset run() {
          return Datasets.load(uri);
        }
      });
    } catch (IOException | InterruptedException e) {
      throw new SqoopException(KiteConnectorError.GENERIC_KITE_CONNECTOR_0005, "Unexpected exception", e);
    }
    org.apache.avro.Schema avroSchema = dataset.getDescriptor().getSchema();
    return AvroDataTypeUtil.createSqoopSchema(avroSchema);
  }

}