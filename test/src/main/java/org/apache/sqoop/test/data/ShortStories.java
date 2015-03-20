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
public class ShortStories extends DataSet {

  public ShortStories(DatabaseProvider provider, TableName tableBaseName) {
    super(provider, tableBaseName);
  }

  @Override
  public DataSet createTables() {
    provider.createTable(
        tableBaseName,
        "id",
        "id", "int",
        "name", "varchar(64)",
        "story", "varchar(10000)"
    );

    return this;
  }

  @Override
  public DataSet loadBasicData() {
    provider.insertRow(tableBaseName,  1, "The Gift of the Magi",
        "ONE DOLLAR AND EIGHTY-SEVEN CENTS. THAT WAS ALL. AND SIXTY CENTS of it was in pennies. Pennies saved one and two at a time by bulldozing the grocer and the vegetable man and the butcher until ones cheeks burned with the silent imputation of parsimony that such close dealing implied. Three times Della counted it. One dollar and eighty-seven cents. And the next day would be Christmas.\n" +
        "\n" +
        "There was clearly nothing left to do but flop down on the shabby little couch and howl. So Della did it. Which instigates the moral reflection that life is made up of sobs, sniffles, and smiles, with sniffles predominating.");
    provider.insertRow(tableBaseName,  2, "The Little Match Girl",
        "Most terribly cold it was; it snowed, and was nearly quite dark, and evening-- the last evening of the year. In this cold and darkness there went along the street a poor little girl, bareheaded, and with naked feet. When she left home she had slippers on, it is true; but what was the good of that? They were very large slippers, which her mother had hitherto worn; so large were they; and the poor little thing lost them as she scuffled away across the street, because of two carriages that rolled by dreadfully fast.");
    provider.insertRow(tableBaseName,  3, "To Build a Fire",
        "Day had broken cold and grey, exceedingly cold and grey, when the man turned aside from the main Yukon trail and climbed the high earth- bank, where a dim and little-travelled trail led eastward through the fat spruce timberland. It was a steep bank, and he paused for breath at the top, excusing the act to himself by looking at his watch. It was nine oclock. There was no sun nor hint of sun, though there was not a cloud in the sky. It was a clear day, and yet there seemed an intangible pall over the face of things, a subtle gloom that made the day dark, and that was due to the absence of sun. This fact did not worry the man. He was used to the lack of sun. It had been days since he had seen the sun, and he knew that a few more days must pass before that cheerful orb, due south, would just peep above the sky- line and dip immediately from view.");

    return this;
  }
}
