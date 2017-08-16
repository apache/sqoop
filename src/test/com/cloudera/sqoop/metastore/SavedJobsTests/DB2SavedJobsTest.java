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

package com.cloudera.sqoop.metastore.SavedJobsTests;

import org.apache.sqoop.manager.JdbcDrivers;

public class DB2SavedJobsTest extends SavedJobsTest {

    DB2SavedJobsTest () {
        super(System.getProperty(
                "sqoop.test.db2.connectstring.host_url",
                "jdbc:db2://db2host:50000"),
                System.getProperty(
                        "sqoop.test.db2.connectstring.username",
                        "SQOOP"),
                System.getProperty(
                        "sqoop.test.db2.connectstring.password",
                        "SQOOP"),
                JdbcDrivers.DB2.getDriverClass());
    }
}
