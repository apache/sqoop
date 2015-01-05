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
package org.apache.sqoop.repository.postgresql;

import static org.apache.sqoop.repository.postgresql.PostgresqlSchemaConstants.*;

/**
 * DML for PostgreSQL repository.
 */
public class PostgresqlSchemaQuery {

  public static final String STMT_SELECT_SYSTEM =
      "SELECT "
          + COLUMN_SQM_VALUE
          + " FROM " + TABLE_SQ_SYSTEM
          + " WHERE " + COLUMN_SQM_KEY + " = ?";

  public static final String STMT_DELETE_SYSTEM =
      "DELETE FROM "  + TABLE_SQ_SYSTEM
          + " WHERE " + COLUMN_SQM_KEY + " = ?";

  public static final String STMT_INSERT_SYSTEM =
      "INSERT INTO " + TABLE_SQ_SYSTEM + "("
          + COLUMN_SQM_KEY + ", "
          + COLUMN_SQM_VALUE + ") "
          + "VALUES(?, ?)";

  public static final String STMT_INSERT_DIRECTION =
      "INSERT INTO " + TABLE_SQ_DIRECTION
          + " (" + COLUMN_SQD_NAME+ ") VALUES (?)";

  private PostgresqlSchemaQuery() {
    // Disable explicit object creation
  }
}
