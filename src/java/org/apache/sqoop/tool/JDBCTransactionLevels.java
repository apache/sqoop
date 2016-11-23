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

import java.sql.Connection;

enum JDBCTransactionLevels {
  TRANSACTION_NONE(Connection.TRANSACTION_NONE),
  TRANSACTION_READ_UNCOMMITTED(Connection.TRANSACTION_READ_UNCOMMITTED),
  TRANSACTION_READ_COMMITTED(Connection.TRANSACTION_READ_COMMITTED),
  TRANSACTION_REPEATABLE_READ(Connection.TRANSACTION_REPEATABLE_READ),
  TRANSACTION_SERIALIZABLE(Connection.TRANSACTION_SERIALIZABLE);

  private final int transactionLevelValue;

  private JDBCTransactionLevels(int transactionLevelValue) {
    this.transactionLevelValue = transactionLevelValue;
  }

  public int getTransactionIsolationLevelValue() {
    return transactionLevelValue;
  }
}
