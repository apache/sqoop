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

package org.apache.sqoop.manager;

public enum JdbcDrivers {
    MYSQL("com.mysql.jdbc.Driver", "jdbc:mysql:"), POSTGRES("org.postgresql.Driver", "jdbc:postgresql:"),
    HSQLDB("org.hsqldb.jdbcDriver","jdbc:hsqldb:"), ORACLE("oracle.jdbc.OracleDriver","jdbc:oracle:"),
    SQLSERVER("com.microsoft.sqlserver.jdbc.SQLServerDriver", "jdbc:sqlserver:"),
    JTDS_SQLSERVER("net.sourceforge.jtds.jdbc.Driver", "jdbc:jtds:sqlserver:"),
    DB2("com.ibm.db2.jcc.DB2Driver", "jdbc:db2:"), NETEZZA("org.netezza.Driver", "jdbc:netezza:"),
    CUBRID("cubrid.jdbc.driver.CUBRIDDriver", "jdbc:cubrid:");

    private final String driverClass;
    private final String schemePrefix;

    JdbcDrivers(String driverClass, String schemePrefix) {
        this.driverClass = driverClass;
        this.schemePrefix = schemePrefix;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public String getSchemePrefix() {
        return schemePrefix;
    }
}
