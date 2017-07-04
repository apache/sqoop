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

import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduTable;

/**
 * Interface that takes a map of jdbc field names to values and converts
 * them to an Insert Operation for Kudu.
 */
public abstract class MutationTransformer {

  private KuduTable kuduTable;

  /**
   * Returns the Kudu table to insert into.
   */
  public KuduTable getKuduTable() {
    return this.kuduTable;
  }

  /**
   * Sets the Kudu table to insert into.
   *
   * @param kuduTable
   */
  public void setKuduTable(KuduTable kuduTable) {

    this.kuduTable = kuduTable;
  }

  /**
   * Returns a list of Put commands that inserts the fields into a row in
   * HBase.
   *
   * @param fields a map of field names to values to insert.
   * @return A list of Put commands that inserts these into HBase.
   */
  public abstract List<Insert> getInsertCommand(Map<String, Object> fields)
      throws IOException;

}
