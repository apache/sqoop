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

package org.apache.sqoop.util;

import org.junit.runner.RunWith;
import org.junit.runners.model.InitializationError;
import org.junit.runners.parameterized.TestWithParameters;

import java.lang.annotation.Annotation;

public class BlockJUnit4ClassRunnerWithParameters extends org.junit.runners.parameterized.BlockJUnit4ClassRunnerWithParameters {
  public BlockJUnit4ClassRunnerWithParameters(TestWithParameters test) throws InitializationError {
    super(test);
  }

  @Override
  protected Annotation[] getRunnerAnnotations() {
    Annotation[] allAnnotations = getTestClass().getAnnotations();
    Annotation[] annotationsWithoutRunWith = new Annotation[allAnnotations.length - 1];
    int i = 0;
    for (Annotation annotation: allAnnotations) {
      if (!annotation.annotationType().equals(RunWith.class)) {
        annotationsWithoutRunWith[i] = annotation;
        ++i;
      }
    }
    return annotationsWithoutRunWith;
  }
}
