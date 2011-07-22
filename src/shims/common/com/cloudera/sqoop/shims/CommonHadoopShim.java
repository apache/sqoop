/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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

package com.cloudera.sqoop.shims;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Contains code which belongs in all Hadoop shims which is syntactically
 * identical in both, but needs to be recompiled against multiple different
 * Hadoop versions (e.g., references to 'final static String' fields).
 */
public abstract class CommonHadoopShim extends HadoopShim {

  @Override
  public String [] parseGenericOptions(Configuration conf, String [] args)
      throws IOException {
    // This needs to be shimmed because in Apache Hadoop this can throw
    // an IOException, but it does not do so in CDH. We just mandate in
    // this method that an IOException is possible.
    GenericOptionsParser genericParser = new GenericOptionsParser(
        conf, args);
    return genericParser.getRemainingArgs();
  }

  @Override
  public String getDbInputClassProperty() {
    return DBConfiguration.INPUT_CLASS_PROPERTY;
  }

  @Override
  public String getDbUsernameProperty() {
    return DBConfiguration.USERNAME_PROPERTY;
  }

  @Override
  public String getDbPasswordProperty() {
    return DBConfiguration.PASSWORD_PROPERTY;
  }

  @Override
  public String getDbUrlProperty() {
    return DBConfiguration.URL_PROPERTY;
  }

  @Override
  public String getDbInputTableNameProperty() {
    return DBConfiguration.INPUT_TABLE_NAME_PROPERTY;
  }

  @Override
  public String getDbInputConditionsProperty() {
    return DBConfiguration.INPUT_CONDITIONS_PROPERTY;
  }
}

