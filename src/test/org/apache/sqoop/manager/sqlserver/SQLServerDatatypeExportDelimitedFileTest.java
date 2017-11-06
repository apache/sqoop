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
package org.apache.sqoop.manager.sqlserver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.sqoop.manager.sqlserver.MSSQLTestDataFileParser.DATATYPES;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.BufferedWriter;

/**
 * Test to export delimited file to SQL Server.
 *
 * This uses JDBC to export data to an SQLServer database from HDFS.
 *
 * Since this requires an SQLServer installation,
 * this class is named in such a way that Sqoop's default QA process does
 * not run it. You need to run this manually with
 * -Dtestcase=SQLServerDatatypeExportDelimitedFileTest or -Dthirdparty=true.
 *
 * You need to put SQL Server JDBC driver library (sqljdbc4.jar) in a location
 * where Sqoop will be able to access it (since this library cannot be checked
 * into Apache's tree for licensing reasons) and set it's path through -Dsqoop.thirdparty.lib.dir.
 *
 * To set up your test environment:
 *   Install SQL Server Express 2012
 *   Create a database SQOOPTEST
 *   Create a login SQOOPUSER with password PASSWORD and grant all
 *   access for SQOOPTEST to SQOOPUSER.
 *   Set these through -Dsqoop.test.sqlserver.connectstring.host_url, -Dsqoop.test.sqlserver.database and
 *   -Dms.sqlserver.password
 */
public class SQLServerDatatypeExportDelimitedFileTest
    extends ManagerCompatExport {

  @Override
  public void createFile(DATATYPES dt, String[] data) throws IOException {
    Path tablePath = getTablePath(dt);
    Path filePath = new Path(tablePath, "part0000");

    Configuration conf = new Configuration();
    String hdfsroot;
    hdfsroot = System.getProperty("ms.datatype.test.hdfsprefix");
    if (hdfsroot == null) {
      hdfsroot = "hdfs://localhost/";
    }
    conf.set("fs.default.name", hdfsroot);
    FileSystem fs = FileSystem.get(conf);
    fs.mkdirs(tablePath);
    System.out.println("-----------------------------------Path : "
        + filePath);
    OutputStream os = fs.create(filePath);

    BufferedWriter w = new BufferedWriter(new OutputStreamWriter(os));
    for (int i = 0; i < data.length; i++) {
      w.write(data[i] + "\n");
    }
    w.close();
    os.close();
  }

  @Override
  public void createFile(DATATYPES dt, String data) throws IOException {
    createFile(dt, new String[] { data });
  }

  @Override
  public String getOutputFileName() {
    return "ManagerCompatExportDelim.txt";
  }

}
