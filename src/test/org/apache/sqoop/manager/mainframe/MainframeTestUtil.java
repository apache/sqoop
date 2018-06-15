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

package org.apache.sqoop.manager.mainframe;

public class MainframeTestUtil {
  public static final String HOST = System.getProperty(
      "sqoop.test.mainframe.ftp.host",
      "localhost");
  public static final String PORT = System.getProperty(
      "sqoop.test.mainframe.ftp.port",
      "2121");
  public static final String USERNAME = System.getProperty(
      "sqoop.test.mainframe.ftp.username",
      "test");
  public static final String PASSWORD = System.getProperty(
      "sqoop.test.mainframe.ftp.password",
      "test");
  public static final String GDG_DATASET_NAME = System.getProperty(
      "sqoop.test.mainframe.ftp.dataset.gdg",
      "TSODIQ1.GDGTEXT");
  public static final String GDG_DATASET_FILENAME = System.getProperty(
      "sqoop.test.mainframe.ftp.dataset.gdg.filename",
      "G0001V43"
      );
  public static final String EXPECTED_GDG_DATASET_MD5 = System.getProperty(
      "sqoop.test.mainframe.ftp.dataset.gdg.md5",
      "f0d0d171fdb8a03dbc1266ed179d7093");
}
