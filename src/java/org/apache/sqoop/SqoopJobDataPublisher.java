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

package org.apache.sqoop;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sqoop.mapreduce.ExportJobBase;
import org.apache.sqoop.mapreduce.ImportJobBase;
import org.apache.sqoop.mapreduce.hcat.SqoopHCatUtilities;

import java.util.Properties;

/**
 * Publisher class for publising data to a consumer upon completion of Sqoop actions.
 * Currently supports Hive import actions only.
 */
public class SqoopJobDataPublisher {

    public static class Data {

        public static final String JDBC_STORE = "JDBCStore";

        String operation;
        String user;
        String storeType;
        String storeTable;
        String storeQuery;
        String hiveDB;
        String hiveTable;
        Properties commandLineOpts;
        long startTime;
        long endTime;

        String url;

        public String getOperation() {
            return operation;
        }

        public String getUser() {
            return user;
        }

        public String getStoreType() {
            return storeType;
        }

        public String getStoreTable() {
            return storeTable;
        }

        public String getStoreQuery() {
            return storeQuery;
        }

        public String getHiveDB() {
            return hiveDB;
        }

        public String getHiveTable() {
            return hiveTable;
        }

        public Properties getOptions() {
            return commandLineOpts;
        }

        public String getUrl() {
            return url;
        }

        public long getStartTime() { return startTime; }

        public long getEndTime() { return endTime; }

        private void init(String operation, String url, String user, String storeType, String storeTable,
                          String storeQuery, String hiveDB, String hiveTable, Properties commandLineOpts,
                          long startTime, long endTime) {
            this.operation = operation;
            this.url = url;
            this.user = user;
            this.storeType = storeType;
            this.storeTable = (storeTable == null) ? hiveTable : storeTable;
            this.storeQuery = storeQuery;
            this.hiveDB = (hiveDB == null) ? SqoopHCatUtilities.DEFHCATDB : hiveDB;
            this.hiveTable = hiveTable;
            this.commandLineOpts = commandLineOpts;
            this.startTime = startTime;
            this.endTime = endTime;
        }

        public Data(String operation, String url, String user, String storeType,
                    String storeTable, String storeQuery, String hiveDB, String hiveTable,
                    Properties commandLineOpts, long startTime, long endTime) throws Exception{
            init(operation, url, user, storeType, storeTable, storeQuery,
                    hiveDB, hiveTable, commandLineOpts, startTime, endTime);
        }

        public Data(String operation, SqoopOptions options, String tableName,
                    long startTime, long endTime) throws Exception {
            String hiveTableName = null;
            String hiveDatabase = null;
            if (ExportJobBase.OPERATION.equals(operation)) {
                // export job data
                hiveTableName = options.getHCatTableName();
                hiveDatabase = options.getHCatDatabaseName();
            } else if (ImportJobBase.OPERATION.equals(operation)){
                // import job data
                hiveTableName = options.doHiveImport() ?
                        options.getHiveTableName() : options.getHCatTableName();
                hiveDatabase = options.doHiveImport() ?
                        options.getHiveDatabaseName() : options.getHCatDatabaseName();
            } else {
                throw new Exception("Data published for unsupported Operation "
                        + operation + " in SqoopJobDataPublisher");
            }

            String dataStoreType = JDBC_STORE;
            String[] storeTypeFields = options.getConnectString().split(":");
            if (storeTypeFields.length > 2) {
                dataStoreType = storeTypeFields[1];
            }

            init(operation, options.getConnectString(), UserGroupInformation.getCurrentUser().getShortUserName(),
                    dataStoreType, tableName, options.getSqlQuery(), hiveDatabase, hiveTableName,
                    options.writeProperties(), startTime, endTime);
        }

        public String toString() {
            return  "Operation=" + operation + ", Url=" + url + ", User=" + user + ", StoreType=" + storeType
                    + ", StoreTable=" + storeTable + ", StoreQuery=" + storeQuery + ", HiveDB=" + hiveDB
                    + ", HiveTable=" + hiveTable + ", StartTime=" + startTime + ", EndTime=" + endTime
                    + ", CmdLineArgs=" + commandLineOpts;
        }
    }

    public void publish(Data data) throws Exception{

    }
}
