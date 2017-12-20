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

package org.apache.sqoop.metastore.hsqldb;

import org.apache.sqoop.metastore.MetaConnectIncrementalImportTestBase;

/**
 * Test that Incremental-Import values are stored correctly in Hsqldb
 *
 * This class is named in such a way that Sqoop's default QA process does
 * not run it. You need to run this manually with
 * -Dtestcase=HsqldbMetaConnectIncrementalImportTest or -Dthirdparty=true.
 *
 * This uses JDBC to store and retrieve metastore data from a local Hsqldb server
 */

public class HsqldbMetaConnectIncrementalImportTest extends MetaConnectIncrementalImportTestBase {

    public HsqldbMetaConnectIncrementalImportTest() {
        super( "jdbc:hsqldb:mem:sqoopmetastore", "SA" , "");
    }
}
