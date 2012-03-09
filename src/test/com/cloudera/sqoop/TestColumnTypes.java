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

package com.cloudera.sqoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.sqoop.testutil.ManagerCompatTestCase;

/**
 * Test that each of the different SQL Column types that we support
 * can, in fact, be imported into HDFS. Test that the writable
 * that we expect to work, does.
 *
 * This uses hsqldb as its test database.
 *
 * This requires testing:
 * - That we can pull from the database into HDFS:
 *    readFields(ResultSet), toString()
 * - That we can pull from mapper to reducer:
 *    write(DataOutput), readFields(DataInput)
 * - And optionally, that we can push to the database:
 *    write(PreparedStatement)
 */
public class TestColumnTypes extends ManagerCompatTestCase {

  public static final Log LOG = LogFactory.getLog(
      TestColumnTypes.class.getName());

  @Override
  protected Log getLogger() {
    return LOG;
  }

  @Override
  protected String getDbFriendlyName() {
    return "HSQLDB";
  }

  @Override
  protected boolean useHsqldbTestServer() {
    return true;
  }

  // Don't need to override getConnectString() because the default uses hsqldb.

  // HSQLdb does not support these types over JDBC.

  @Override
  protected boolean supportsClob() {
    return false;
  }

  @Override
  protected boolean supportsBlob() {
    return false;
  }

  @Override
  protected String getVarBinarySeqOutput(String asInserted) {
    return toLowerHexString(asInserted);
  }
}

