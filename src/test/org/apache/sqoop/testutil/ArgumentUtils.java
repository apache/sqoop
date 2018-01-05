/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.testutil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.commons.lang3.StringUtils.isEmpty;

public final class ArgumentUtils {

  private static final String PROPERTY_PREFIX = "-D";

  private static final String OPTION_PREFIX = "--";

  public static String[] createArgumentArrayFromProperties(Iterable<Argument> properties) {
    List<String> result = new ArrayList<>();
    for (Argument property : properties) {
      result.add(PROPERTY_PREFIX);
      result.add(property.toString());
    }

    return result.toArray(new String[result.size()]);
  }

  public static String[] createArgumentArrayFromOptions(Iterable<Argument> options) {
    List<String> result = new ArrayList<>();
    for (Argument option : options) {
      result.add(OPTION_PREFIX + option.getName());
      if (!isEmpty(option.getValue())) {
        result.add(option.getValue());
      }
    }

    return result.toArray(new String[result.size()]);
  }

  public static String[] createArgumentArray(Iterable<Argument> properties, Iterable<Argument> options) {
    List<String> result = new ArrayList<>();
    Collections.addAll(result, createArgumentArrayFromProperties(properties));
    Collections.addAll(result, createArgumentArrayFromOptions(options));

    return result.toArray(new String[result.size()]);
  }

}
