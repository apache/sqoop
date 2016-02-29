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
package org.apache.sqoop.test.data;

import org.apache.sqoop.common.test.db.DatabaseProvider;
import org.apache.sqoop.common.test.db.TableName;

import java.sql.Timestamp;

/**
 * Releases of Ubuntu Linux.
 *
 * Purpose of this set is to cover most common data types (varchar, int, decimal, timestamp).
 */
public class UbuntuReleases extends DataSet {

  public UbuntuReleases(DatabaseProvider provider, TableName tableBaseName) {
    super(provider, tableBaseName);
  }

  @Override
  public DataSet createTables() {
    provider.createTable(
      tableBaseName,
      "id",
      "id", "int",
      "code_name", "varchar(64)",
      "version", "numeric(4,2)",
      "release_date", provider.getDateTimeType()
    );

    return this;
  }

  @Override
  public DataSet loadBasicData() {
    provider.insertRow(tableBaseName,  1, "Warty Warthog",    4.10,  Timestamp.valueOf("2004-10-20 00:00:00"));
    provider.insertRow(tableBaseName,  2, "Hoary Hedgehog",   5.04,  Timestamp.valueOf("2005-04-08 00:00:00"));
    provider.insertRow(tableBaseName,  3, "Breezy Badger",    5.10,  Timestamp.valueOf("2005-10-13 00:00:00"));
    provider.insertRow(tableBaseName,  4, "Dapper Drake",     6.06,  Timestamp.valueOf("2006-06-01 00:00:00"));
    provider.insertRow(tableBaseName,  5, "Edgy Eft",         6.10,  Timestamp.valueOf("2006-10-26 00:00:00"));
    provider.insertRow(tableBaseName,  6, "Feisty Fawn",      7.04,  Timestamp.valueOf("2007-04-19 00:00:00"));
    provider.insertRow(tableBaseName,  7, "Gutsy Gibbon",     7.10,  Timestamp.valueOf("2007-10-18 00:00:00"));
    provider.insertRow(tableBaseName,  8, "Hardy Heron",      8.04,  Timestamp.valueOf("2008-04-24 00:00:00"));
    provider.insertRow(tableBaseName,  9, "Intrepid Ibex",    8.10,  Timestamp.valueOf("2008-10-18 00:00:00"));
    provider.insertRow(tableBaseName, 10, "Jaunty Jackalope", 9.04,  Timestamp.valueOf("2009-04-23 00:00:00"));
    provider.insertRow(tableBaseName, 11, "Karmic Koala",     9.10,  Timestamp.valueOf("2009-10-29 00:00:00"));
    provider.insertRow(tableBaseName, 12, "Lucid Lynx",      10.04,  Timestamp.valueOf("2010-04-29 00:00:00"));
    provider.insertRow(tableBaseName, 13, "Maverick Meerkat",10.10,  Timestamp.valueOf("2010-10-10 00:00:00"));
    provider.insertRow(tableBaseName, 14, "Natty Narwhal",   11.04,  Timestamp.valueOf("2011-04-28 00:00:00"));
    provider.insertRow(tableBaseName, 15, "Oneiric Ocelot",  11.10,  Timestamp.valueOf("2011-10-10 00:00:00"));
    provider.insertRow(tableBaseName, 16, "Precise Pangolin",12.04,  Timestamp.valueOf("2012-04-26 00:00:00"));
    provider.insertRow(tableBaseName, 17, "Quantal Quetzal", 12.10,  Timestamp.valueOf("2012-10-18 00:00:00"));
    provider.insertRow(tableBaseName, 18, "Raring Ringtail", 13.04,  Timestamp.valueOf("2013-04-25 00:00:00"));
    provider.insertRow(tableBaseName, 19, "Saucy Salamander",13.10,  Timestamp.valueOf("2013-10-17 00:00:00"));

    return this;
  }
}
