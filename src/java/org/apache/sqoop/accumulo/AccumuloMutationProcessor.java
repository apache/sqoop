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

package org.apache.sqoop.accumulo;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import com.cloudera.sqoop.lib.FieldMapProcessor;
import com.cloudera.sqoop.lib.FieldMappable;
import com.cloudera.sqoop.lib.ProcessingException;

/**
 * SqoopRecordProcessor that performs an Accumulo mutation operation
 * that contains all the fields of the record.
 */
public class AccumuloMutationProcessor implements Closeable, Configurable,
    FieldMapProcessor {

  private Configuration conf;

  // An object that can transform a map of fieldName->object
  // into a Mutation.
  private MutationTransformer mutationTransformer;

  private String tableName;
  private BatchWriter table;

  public AccumuloMutationProcessor() {
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setConf(Configuration config) {
    this.conf = config;

    // Get the implementation of MutationTransformer to use.
    // By default, we call toString() on every non-null field.
    Class<? extends MutationTransformer> xformerClass =
        (Class<? extends MutationTransformer>)
        this.conf.getClass(AccumuloConstants.TRANSFORMER_CLASS_KEY,
        ToStringMutationTransformer.class);
    this.mutationTransformer = (MutationTransformer)
        ReflectionUtils.newInstance(xformerClass, this.conf);
    if (null == mutationTransformer) {
      throw new RuntimeException("Could not instantiate MutationTransformer.");
    }

    String colFam = conf.get(AccumuloConstants.COL_FAMILY_KEY, null);
    if (null == colFam) {
      throw new RuntimeException("Accumulo column family not set.");
    }
    this.mutationTransformer.setColumnFamily(colFam);

    String rowKey = conf.get(AccumuloConstants.ROW_KEY_COLUMN_KEY, null);
    if (null == rowKey) {
      throw new RuntimeException("Row key column not set.");
    }
    this.mutationTransformer.setRowKeyColumn(rowKey);

    String vis = conf.get(AccumuloConstants.VISIBILITY_KEY, null);
    this.mutationTransformer.setVisibility(vis);

    this.tableName = conf.get(AccumuloConstants.TABLE_NAME_KEY, null);
    String zookeeper = conf.get(AccumuloConstants.ZOOKEEPERS);
    String instance = conf.get(AccumuloConstants.ACCUMULO_INSTANCE);

    Instance inst = new ZooKeeperInstance(instance, zookeeper);
    String username = conf.get(AccumuloConstants.ACCUMULO_USER_NAME);
    String pw = conf.get(AccumuloConstants.ACCUMULO_PASSWORD);
    if (null == pw) {
      pw = "";
    }
    byte[] password = pw.getBytes();

    BatchWriterConfig bwc = new BatchWriterConfig();

    long bs = conf.getLong(AccumuloConstants.BATCH_SIZE,
       AccumuloConstants.DEFAULT_BATCH_SIZE);
    bwc.setMaxMemory(bs);

    long la = conf.getLong(AccumuloConstants.MAX_LATENCY,
       AccumuloConstants.DEFAULT_LATENCY);
    bwc.setMaxLatency(la, TimeUnit.MILLISECONDS);

    try {
      Connector conn = inst.getConnector(username, new PasswordToken(password));

      this.table = conn.createBatchWriter(tableName, bwc);
    } catch (AccumuloException ex) {
      throw new RuntimeException("Error accessing Accumulo", ex);
    } catch (AccumuloSecurityException aex){
      throw new RuntimeException("Security exception accessing Accumulo", aex);
    } catch(TableNotFoundException tex){
      throw new RuntimeException("Accumulo table " + tableName
           + " not found", tex);
    }
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  /**
   * Processes a record by extracting its field map and converting
   * it into a list of Mutations into Accumulo.
   */
  public void accept(FieldMappable record)
      throws IOException, ProcessingException {
    Map<String, Object> fields = record.getFieldMap();

    Iterable<Mutation> putList = mutationTransformer.getMutations(fields);
    if (null != putList) {
      for (Mutation m : putList) {
        try {
          this.table.addMutation(m);
        } catch (MutationsRejectedException ex) {
          throw new IOException("Mutation rejected" , ex);
        }
      }
    }
  }

  @Override
  /**
   * Closes the Accumulo table and commits all pending operations.
   */
  public void close() throws IOException {
    try {
      this.table.close();
    } catch (MutationsRejectedException ex) {
      throw new IOException("Mutations rejected", ex);
    }
  }
}
