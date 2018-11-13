/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.testcategories.sqooptest.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

import static org.apache.sqoop.metastore.AutoGenericJobStorage.AUTO_STORAGE_IS_ACTIVE_KEY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(UnitTest.class)
public class TestAutoGenericJobStorage {

  private AutoGenericJobStorage jobStorage;

  private Configuration jobStorageConfiguration;

  private Map<String, String> descriptor;

  @Before
  public void before() {
    jobStorage = new AutoGenericJobStorage();
    jobStorageConfiguration = new Configuration();
    descriptor = new HashMap<>();

    jobStorage.setConf(jobStorageConfiguration);
  }

  @Test
  public void testCanAcceptWithAutoStorageDisabledReturnsFalse() {
    jobStorageConfiguration.setBoolean(AUTO_STORAGE_IS_ACTIVE_KEY, false);
    assertFalse(jobStorage.canAccept(descriptor));
  }

  @Test
  public void testCanAcceptWithAutoStorageEnabledReturnsTrue() {
    jobStorageConfiguration.setBoolean(AUTO_STORAGE_IS_ACTIVE_KEY, true);
    assertTrue(jobStorage.canAccept(descriptor));
  }

  @Test
  public void testCanAcceptWithAutoStorageDefaultValueReturnsTrue() {
    assertTrue(jobStorage.canAccept(descriptor));
  }

}
