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

package org.apache.sqoop.importjob.configuration;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * This test configuration intends to cover the fact that oracle stores these types without padding them with 0s,
 * therefore when importing into avro, one has to use the padding feature.
 */
public class GenericImportJobSplitByTestConfiguration implements ImportJobTestConfiguration, ParquetTestConfiguration {

  public static final String NAME_COLUMN = "NAME";
  public static final char SEPARATOR = ',';

  List<String[]> data = new ArrayList<>();
  {
    data.add(new String[]{"ID_1", "Mr T."});
    data.add(new String[]{"ID_2", "D'Artagnan"});
    data.add(new String[]{"ID_3", "Jean D'Arc"});
    data.add(new String[]{"ID_4", "Jeremy Renner"});
  }

  List<String[]> escapedData = new ArrayList<>();
  {
    escapedData.add(new String[]{"'ID_1'", "'Mr T.'"});
    escapedData.add(new String[]{"'ID_2'", "'D''Artagnan'"});
    escapedData.add(new String[]{"'ID_3'", "'Jean D''Arc'"});
    escapedData.add(new String[]{"'ID_4'", "'Jeremy Renner'"});
  }

  @Override
  public String[] getTypes() {
    return new String[]{"VARCHAR(20)", "VARCHAR(20)"};
  }

  @Override
  public String[] getNames() {
    return new String[]{"ID", NAME_COLUMN};
  }

  @Override
  public List<String[]> getSampleData() {
    return new ArrayList<>(escapedData);
  }

  @Override
  public String[] getExpectedResultsForParquet() {
    return data.stream()
        .map(element -> StringUtils.join(element, SEPARATOR))
        .toArray(String[]::new);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
