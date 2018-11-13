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

package org.apache.sqoop.mapreduce.hcat;

import static java.lang.Thread.currentThread;
import static org.junit.Assert.assertSame;

import java.io.IOException;

import org.apache.sqoop.testcategories.sqooptest.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class TestSqoopHCatUtilities {

  private static final String[] VALID_HCAT_ARGS = new String[] {"-h"};
  private static final String[] INVALID_HCAT_ARGS = new String[] {};

  private SqoopHCatUtilities sqoopHCatUtilities;
  private ClassLoader originalClassLoader;

  @Before
  public void before() {
    sqoopHCatUtilities = SqoopHCatUtilities.instance();
    originalClassLoader = currentThread().getContextClassLoader();
  }

  @Test
  public void testOriginalClassLoaderIsRestoredWhenHCatCliIsInvokedWithValidArguments() throws IOException {
    sqoopHCatUtilities.executeHCatProgramInProcess(VALID_HCAT_ARGS);

    assertSame(originalClassLoader, currentThread().getContextClassLoader());
  }

  @Test
  public void testOriginalClassLoaderIsRestoredWhenHCatCliIsInvokedWithInvalidArguments() {
    try {
      sqoopHCatUtilities.executeHCatProgramInProcess(INVALID_HCAT_ARGS);
    } catch (IOException e) {
      // Exception is swallowed because we test the classloader value only.
    }

    assertSame(originalClassLoader, currentThread().getContextClassLoader());
  }

  @Test(expected = IOException.class)
  public void testExecuteHCatProgramInProcessThrowsWithInvalidArguments() throws IOException {
    sqoopHCatUtilities.executeHCatProgramInProcess(INVALID_HCAT_ARGS);
  }
}