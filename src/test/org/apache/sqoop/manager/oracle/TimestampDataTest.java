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

package org.apache.sqoop.manager.oracle;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * These tests need to be separate as changing the mapping type for timestamp
 * requires the tests to be run in a different process. Maven needs to be setup
 * to fork per test class.
 */
public class TimestampDataTest extends OraOopTestCase {

  @Test
  public void testProductImportTimezone() throws Exception {
    setSqoopTargetDirectory(getSqoopTargetDirectory() + "tst_product_timezone");
    createTable("table_tst_product.xml");

    Configuration sqoopConf = getSqoopConf();
    sqoopConf.setBoolean(OraOopConstants.ORAOOP_MAP_TIMESTAMP_AS_STRING, false);

    try {
      int retCode = runImport("tst_product", sqoopConf, false);
      Assert.assertEquals("Return code should be 0", 0, retCode);

    } finally {
      cleanupFolders();
      closeTestEnvConnection();
    }
  }

}
