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

import org.junit.runner.Runner;
import org.junit.runners.model.InitializationError;
import org.junit.runners.parameterized.ParametersRunnerFactory;
import org.junit.runners.parameterized.TestWithParameters;

/**
 * Due to an issue in JUnit Gradle does not execute categorized tests which are parameterized too.
 * The issue is already reported(https://github.com/junit-team/junit4/issues/751) but its fix will be released in JUnit 4.13.
 * This factory returns a custom BlockJUnit4ClassRunnerWithParameters instance which contains the fix for this issue so
 * we have to use @Parameterized.UseParametersRunnerFactory(BlockJUnit4ClassRunnerWithParametersFactory.class) annotation
 * on parameterized test classes until JUnit 4.13 is released and we migrate to it.
 */
public class BlockJUnit4ClassRunnerWithParametersFactory implements ParametersRunnerFactory {
  @Override
  public Runner createRunnerForTestWithParameters(TestWithParameters test) throws InitializationError {
    return new BlockJUnit4ClassRunnerWithParameters(test);
  }
}
