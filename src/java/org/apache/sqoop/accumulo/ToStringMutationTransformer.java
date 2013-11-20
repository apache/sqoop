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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

/**
 * PutTransformer that calls toString on all non-null fields.
 */
public class ToStringMutationTransformer extends MutationTransformer {

  public static final Log LOG = LogFactory.getLog(
      ToStringMutationTransformer.class.getName());

  public ToStringMutationTransformer() {
  }

  /**
   * Return the serialized bytes for a field name, using
   * the cache if it's already in there.
   */
  @Override
  public Iterable<Mutation> getMutations(Map<String, Object> fields)
      throws IOException {
    String rowKeyCol = getRowKeyColumn();
    String colFamily = getColumnFamily();
    Object rowKey = fields.get(rowKeyCol);
    String vis = getVisibility();
    if (null == rowKey) {
      // If the row-key column is null, we don't insert this row.
      LOG.warn("Could not insert row with null value for row-key column: "
          + rowKeyCol);
      return null;
    }
    ColumnVisibility colVis = null;
    if (null != vis && vis.length() > 0) {
      colVis = new ColumnVisibility(vis);
    }
    Mutation mut = new Mutation(rowKey.toString());
    for (Map.Entry<String, Object> fieldEntry : fields.entrySet()) {
      String colName = fieldEntry.getKey();
      if (!colName.equals(rowKeyCol)) {
        // This is a regular field, not the row key.
        // Add it if it's not null.
        Object val = fieldEntry.getValue();
        if (null != val) {
          if (null == colVis) {
            mut.put(new Text(colFamily), new Text(colName),
                    new Value(val.toString().getBytes("UTF8")));
          } else {
            mut.put(new Text(colFamily), new Text(colName),
                    colVis, new Value(val.toString().getBytes("UTF8")));
          }
        }
      }
    }
    return Collections.singletonList(mut);
  }
}
