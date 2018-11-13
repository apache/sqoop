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
package org.apache.sqoop.testcategories.sqooptest;

/**
 * SqoopTest includes UnitTest, IntegrationTest and ManualTest.
 *
 * UnitTest:
 * A unit test shall test one class at a time having it's dependencies mocked.
 * A unit test shall not start a mini cluster nor an embedded database and it shall not use a JDBC driver.
 *
 * IntegrationTest:
 * An integration test shall test if independently developed classes work together correctly.
 * An integration test checks a whole scenario and thus may start mini clusters or embedded databases and may connect to external resources like RDBMS instances.
 *
 * ManualTest:
 * Deprecated category, shall not be used nor extended.
 */
public interface SqoopTest {
}
