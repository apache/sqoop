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
package org.apache.sqoop.json;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.json.util.SerializationError;
import org.json.simple.JSONObject;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

/**
 *
 */
public class TestThrowableBean {

  @Test
  public void testSerialization() {
    Throwable ex = new RuntimeException("A");
    ex.initCause(new Exception("B"));

    Throwable retrieved = transfer(ex);

    assertEquals("A", retrieved.getMessage());
    assertEquals(RuntimeException.class, retrieved.getClass());
    assertEquals("B", retrieved.getCause().getMessage());
    assertEquals(Exception.class, retrieved.getCause().getClass());
    assertNull(retrieved.getCause().getCause());
  }

  @Test
  public void testSqoopException() {
    SqoopException ex = new SqoopException(SerializationError.SERIALIZATION_001, "Secret");
    Throwable retrieved = transfer(ex);

    assertNotNull(retrieved);
    assertEquals(SqoopException.class, retrieved.getClass());
    SqoopException sqoopRetrieved = (SqoopException) retrieved;
    assertEquals(SerializationError.class, sqoopRetrieved.getErrorCode().getClass());
    assertEquals(SerializationError.SERIALIZATION_001, sqoopRetrieved.getErrorCode());
    assertEquals("SERIALIZATION_001:Attempt to pass a non-map object to MAP type. - Secret", sqoopRetrieved.getMessage());
  }

  public Throwable transfer(Throwable source) {
    // Serialize it to JSON object
    ThrowableBean bean = new ThrowableBean(source);
    JSONObject json = bean.extract(false);

    // "Move" it across network in text form
    String string = json.toJSONString();

    // Retrieved transferred object
    JSONObject retrievedJson = JSONUtils.parse(string);
    ThrowableBean retrievedBean = new ThrowableBean();
    retrievedBean.restore(retrievedJson);
    return retrievedBean.getThrowable();
  }
}
