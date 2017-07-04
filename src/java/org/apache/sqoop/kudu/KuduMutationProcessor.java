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
package org.apache.sqoop.kudu;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Closeable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.SessionConfiguration.FlushMode;

import com.cloudera.sqoop.lib.FieldMapProcessor;
import com.cloudera.sqoop.lib.FieldMappable;
import com.cloudera.sqoop.lib.ProcessingException;

/**
 * SqoopRecordProcessor that performs an Kudu mutation operation
 * that contains all the fields of the record.
 */
public class KuduMutationProcessor implements Closeable, Configurable,
    FieldMapProcessor {

  public static final Log LOG = LogFactory.getLog(
      KuduMutationProcessor.class.getName());

  /**
   * Configuration key specifying the table to insert into.
   */
  public static final String TABLE_NAME_KEY
      = "sqoop.kudu.insert.table";


  /**
   * Configuration key specifying the MutationTransformer
   * implementation to use.
   */
  public static final String TRANSFORMER_CLASS_KEY =
      "sqoop.kudu.insert.mutation.transformer.class";

  /**
   * Configuration key specifying the KuduMaster URL to use.
   */
  public static final String KUDU_MASTER_KEY =
      "sqoop.kudu.kudu.master";

  private Configuration conf;

  // An object that can transform a map of fieldName->object
  // into a Put command.
  private MutationTransformer mutationTransformer;

  private String kuduMasterURL;
  private String tableName;
  private KuduClient kuduClient;
  private KuduSession kuduSession;
  private KuduTable kuduTable;

  public KuduMutationProcessor() {
  }

  @Override
  public void close() throws IOException {

    try {
      if (null != kuduSession) {
        kuduSession.flush();
      }
    } catch (Exception e) {
      throw new IOException("Error while flushing kudu session");
    } finally {
      try {
        kuduSession.close();
      } catch (Exception e) {
        throw new IOException("Error while closing kudu session");
      }
    }

    try {
      if (null != kuduClient) {
        kuduClient.close();
      }
    } catch (Exception e) {
      throw new IOException("Error closing Kudu client");
    }
  }

  @Override
  public void accept(FieldMappable record) throws IOException,
      ProcessingException {
    Map<String, Object> fields = record.getFieldMap();
    List<Insert> insertList =
        mutationTransformer.getInsertCommand(fields);
    if (null != insertList) {
      for (Insert insert : insertList) {
        if (null != insert) {
          if (null != kuduSession) {
            try {
              kuduSession.apply(insert);
            } catch (Exception e) {
              throw new IOException("Error inserting a row"
                  + e.getMessage());
            }
          } else {
            throw new IOException("Kudu session not initialized");
          }
        }
      }
    }
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setConf(Configuration config) {
    this.conf = config;

    kuduMasterURL = conf.get(KUDU_MASTER_KEY, null);

    if (null == kuduMasterURL) {
      kuduMasterURL = "localhost";
      LOG.warn("Since Kudu Master was not provided, "
          + "defaulting to localhost");
    }


    // Get the implementation of MutationTransformer to use.
    Class<? extends MutationTransformer> xformerClass =
        (Class<? extends MutationTransformer>)
            this.conf.getClass(TRANSFORMER_CLASS_KEY,
                KuduTypeMutationTransformer.class);
    this.mutationTransformer = (MutationTransformer)
        ReflectionUtils.newInstance(xformerClass, this.conf);

    if (null == mutationTransformer) {
      throw new RuntimeException(
          "Could not instantiate MutationTransformer.");
    }


    if (this.mutationTransformer instanceof KuduTypeMutationTransformer) {
      KuduTypeMutationTransformer stringMutationTransformer =
          (KuduTypeMutationTransformer) this.mutationTransformer;

      kuduClient = new KuduClient.KuduClientBuilder(kuduMasterURL)
          .build();
      if (null == kuduClient) {
        throw new RuntimeException(
            "Could not instantiate KuduClient with URI: "
                + kuduMasterURL);
      }
      tableName = conf.get(TABLE_NAME_KEY, null);
      if (null == tableName) {
        LOG.error("Kudu Insert table not provided!");
        throw new RuntimeException("Kudu insert table not provided");
      }

      try {
        kuduTable = kuduClient.openTable(tableName);
      } catch (Exception e) {
        throw new RuntimeException(
            "Could not open Kudu Table: " + tableName);
      }
      stringMutationTransformer.setKuduTable(kuduTable);
      kuduSession = kuduClient.newSession();
      kuduSession.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND);

    }

  }

}
