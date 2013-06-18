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

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.sqoop.manager.sqlserver.MSSQLTestData.KEY_STRINGS;


/**
* Class to parse sql server data types
*/
public class MSSQLTestDataFileParser {

  public static final Log LOG = LogFactory.getLog(
      MSSQLTestDataFileParser.class.getName());

  private String filename;
  private String delim;
  private List records;

  MSSQLTestDataFileParser(String filename) throws Exception {
    this.filename = filename;

  }

  enum DATATYPES {
    DECIMAL, NUMERIC, VARBINARY, TIME, SMALLDATETIME, DATETIME, DATETIME2,
    DATETIMEOFFSET, BIGINT, INT, MONEY, SMALLMONEY, TEXT, NTEXT, NCHAR,
    NVARCHAR, IMAGE, SMALLINT, FLOAT, REAL, DATE, CHAR, VARCHAR, BINARY,
    TINYINT;

  }

  public void parse() throws Exception {
    if (this.filename == null) {
      throw new Exception("No test data file specified.");
    }

    BufferedReader br = new BufferedReader(new FileReader(this.filename));

    if (br != null) {
      records = new ArrayList();

      String tmp;
      String del = this.getDelim();
      int offset = 0;
      while ((tmp = br.readLine()) != null) {
        offset++;
        String[] splits = tmp.split(del);

        if (splits.length == 5 || splits.length == 6
            || splits.length == 7) {
          System.out.println(Integer.toString(offset));
          MSSQLTestData td = new MSSQLTestData(splits[0]);
          td.setData(KEY_STRINGS.OFFSET, Integer.toString(offset));

          if (splits[0].equals(DATATYPES.DECIMAL.toString())
              || splits[0].equals(DATATYPES.NUMERIC.toString())) {

            td.setData(KEY_STRINGS.TO_INSERT, splits[1]);
            td.setData(KEY_STRINGS.DB_READBACK, splits[2]);
            td.setData(KEY_STRINGS.HDFS_READBACK, splits[3]);
            td.setData(KEY_STRINGS.SCALE, splits[4]);
            td.setData(KEY_STRINGS.PREC, splits[5]);
            td.setData(KEY_STRINGS.NEG_POS_FLAG, splits[6]);

            records.add(td);
          } else if (splits[0].equals(DATATYPES.NCHAR.toString())
              || splits[0].equals(DATATYPES.VARBINARY.toString())
              || splits[0].equals(DATATYPES.NVARCHAR.toString())
              || splits[0].equals(DATATYPES.CHAR.toString())
              || splits[0].equals(DATATYPES.VARCHAR.toString())
              || splits[0].equals(DATATYPES.BINARY.toString())) {

            td.setData(KEY_STRINGS.TO_INSERT, splits[1]);
            td.setData(KEY_STRINGS.DB_READBACK, splits[2]);
            td.setData(KEY_STRINGS.HDFS_READBACK, splits[3]);
            td.setData(KEY_STRINGS.SCALE, splits[4]);
            td.setData(KEY_STRINGS.NEG_POS_FLAG, splits[5]);

            records.add(td);

          } else {
            td.setData(KEY_STRINGS.TO_INSERT, splits[1]);
            td.setData(KEY_STRINGS.DB_READBACK, splits[2]);
            td.setData(KEY_STRINGS.HDFS_READBACK, splits[3]);
            td.setData(KEY_STRINGS.NEG_POS_FLAG, splits[4]);

            records.add(td);
          }

        }

      }
      System.out.println("\n\n Records" + records.size() + "\n\n");
    }

  }

  public List getRecords() {
    return records;
  }

  public List getTestdata(DATATYPES dt) {
    List l;
    l = new ArrayList();

    if (records != null) {
      for (Iterator<MSSQLTestData> i = records.iterator(); i.hasNext();) {
        MSSQLTestData tmp = i.next();
        if (tmp.getDatatype().equals(dt.toString())) {
          l.add(tmp);
        }
      }
    }

    return l;
  }

  private void trim(String[] strings) {
    for (int i = 0; i < strings.length; i++) {
      strings[i] = strings[i].trim();
    }

  }

  public String getDelim() {
    return delim;
  }

  public void setDelim(String delim1) {
    this.delim = delim1;
  }

}
