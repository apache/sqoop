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

import com.cloudera.sqoop.SqoopOptions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import static org.hamcrest.CoreMatchers.sameInstance;
import static org.mockito.Mockito.mock;

public class TestBaseSqoopTool {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private BaseSqoopTool testBaseSqoopTool;
  private SqoopOptions testSqoopOptions;

  @Before
  public void setup() {
    testBaseSqoopTool = mock(BaseSqoopTool.class, Mockito.CALLS_REAL_METHODS);
    testSqoopOptions = new SqoopOptions();
  }

  @Test
  public void testRethrowIfRequiredWithoutRethrowPropertySetOrThrowOnErrorOption() {
    testSqoopOptions.setThrowOnError(false);

    testBaseSqoopTool.rethrowIfRequired(testSqoopOptions, new Exception());
  }

  @Test
  public void testRethrowIfRequiredWithRethrowPropertySetAndRuntimeException() {
    RuntimeException expectedException = new RuntimeException();
    testSqoopOptions.setThrowOnError(true);


    exception.expect(sameInstance(expectedException));
    testBaseSqoopTool.rethrowIfRequired(testSqoopOptions, expectedException);
  }

  @Test
  public void testRethrowIfRequiredWithRethrowPropertySetAndException() {
    Exception expectedCauseException = new Exception();
    testSqoopOptions.setThrowOnError(true);

    exception.expect(RuntimeException.class);
    exception.expectCause(sameInstance(expectedCauseException));
    testBaseSqoopTool.rethrowIfRequired(testSqoopOptions, expectedCauseException);
  }

}
