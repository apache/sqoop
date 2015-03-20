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

/**
 * Releases of Ubuntu Linux.
 *
 * Purpose of this set is to cover most common data types (varchar, int, numeric, date, boolean).
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
      "release_date", "date",
      "lts", "boolean"
    );

    return this;
  }

  @Override
  public DataSet loadBasicData() {
    provider.insertRow(tableBaseName,  1, "Warty Warthog",    4.10,  "2004-10-20", false);
    provider.insertRow(tableBaseName,  2, "Hoary Hedgehog",   5.04,  "2005-04-08", false);
    provider.insertRow(tableBaseName,  3, "Breezy Badger",    5.10,  "2005-10-13", false);
    provider.insertRow(tableBaseName,  4, "Dapper Drake",     6.06,  "2006-06-01", true);
    provider.insertRow(tableBaseName,  5, "Edgy Eft",         6.10,  "2006-10-26", false);
    provider.insertRow(tableBaseName,  6, "Feisty Fawn",      7.04,  "2007-04-19", false);
    provider.insertRow(tableBaseName,  7, "Gutsy Gibbon",     7.10,  "2007-10-18", false);
    provider.insertRow(tableBaseName,  8, "Hardy Heron",      8.04,  "2008-04-24", true);
    provider.insertRow(tableBaseName,  9, "Intrepid Ibex",    8.10,  "2008-10-18", false);
    provider.insertRow(tableBaseName, 10, "Jaunty Jackalope", 9.04,  "2009-04-23", false);
    provider.insertRow(tableBaseName, 11, "Karmic Koala",     9.10,  "2009-10-29", false);
    provider.insertRow(tableBaseName, 12, "Lucid Lynx",      10.04,  "2010-04-29", true);
    provider.insertRow(tableBaseName, 13, "Maverick Meerkat",10.10,  "2010-10-10", false);
    provider.insertRow(tableBaseName, 14, "Natty Narwhal",   11.04,  "2011-04-28", false);
    provider.insertRow(tableBaseName, 15, "Oneiric Ocelot",  11.10,  "2011-10-10", false);
    provider.insertRow(tableBaseName, 16, "Precise Pangolin",12.04,  "2012-04-26", true);
    provider.insertRow(tableBaseName, 17, "Quantal Quetzal", 12.10,  "2012-10-18", false);
    provider.insertRow(tableBaseName, 18, "Raring Ringtail", 13.04,  "2013-04-25", false);
    provider.insertRow(tableBaseName, 19, "Saucy Salamander",13.10,  "2013-10-17", false);

    return this;
  }
}
