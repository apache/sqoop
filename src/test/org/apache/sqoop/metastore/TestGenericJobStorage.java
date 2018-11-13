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

import org.apache.sqoop.testcategories.sqooptest.UnitTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.sqoop.metastore.GenericJobStorage.META_CONNECT_KEY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(UnitTest.class)
public class TestGenericJobStorage {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private GenericJobStorage jobStorage;

  private Map<String, String> descriptor;

  @Before
  public void before() {
    jobStorage = new GenericJobStorage();
    descriptor = new HashMap<>();
  }

  @Test
  public void testCanAcceptWithMetaConnectStringSetReturnsTrue() {
    descriptor.put(META_CONNECT_KEY, "anyvalue");
    assertTrue(jobStorage.canAccept(descriptor));
  }

  @Test
  public void testCanAcceptWithoutMetaConnectStringSetReturnsFalse() {
    assertFalse(jobStorage.canAccept(descriptor));
  }

  /**
   * This method validates that the public open() method invokes the connection string validation before connecting.
   * For detailed testing of the validation check TestGenericJobStorageValidate test class.
   * @see org.apache.sqoop.metastore.TestGenericJobStorageValidate
   * @throws IOException
   */
  @Test
  public void testOpenWithInvalidConnectionStringThrows() throws IOException {
    String invalidConnectionString = "invalidConnectionString";
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage(invalidConnectionString + " is an invalid connection string or the required RDBMS is not supported.");
    descriptor.put(META_CONNECT_KEY, invalidConnectionString);

    jobStorage.open(descriptor);
  }

}
