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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.isEmpty;

public class ArgumentArrayBuilder {

  private static final String PROPERTY_PREFIX = "-D";

  private static final String OPTION_PREFIX = "--";

  private static final String TOOL_ARG_SEPARATOR = "--";

  private Map<String, Argument> properties;

  private Map<String, Argument> options;

  private Map<String, Argument> toolOptions;

  private boolean withCommonHadoopFlags;

  public ArgumentArrayBuilder() {
    properties = new HashMap<>();
    options = new HashMap<>();
    toolOptions = new HashMap<>();
  }

  public ArgumentArrayBuilder withProperty(String name, String value) {
    properties.put(name, new Argument(name, value));
    return this;
  }

  public ArgumentArrayBuilder withProperty(String name) {
    properties.put(name, new Argument(name));
    return this;
  }

  public ArgumentArrayBuilder withOption(String name, String value) {
    options.put(name, new Argument(name, value));
    return this;
  }

  public ArgumentArrayBuilder withOption(String name) {
    options.put(name, new Argument(name));
    return this;
  }

  public ArgumentArrayBuilder withToolOption(String name, String value) {
    toolOptions.put(name, new Argument(name, value));
    return this;
  }

  public ArgumentArrayBuilder withToolOption(String name) {
    toolOptions.put(name, new Argument(name));
    return this;
  }

  public ArgumentArrayBuilder with(ArgumentArrayBuilder otherBuilder) {
    properties.putAll(otherBuilder.properties);
    options.putAll(otherBuilder.options);
    toolOptions.putAll(otherBuilder.toolOptions);
    return this;
  }

  public ArgumentArrayBuilder withCommonHadoopFlags(boolean b) {
    withCommonHadoopFlags = b;
    return this;
  }

  public ArgumentArrayBuilder withCommonHadoopFlags() {
    withCommonHadoopFlags = true;
    return this;
  }

  /**
   * Transforms the given options, properties and toolOptions to the command line format Sqoop expects,
   * by adding dashes (--) and the capital D letter when it's necessary (in front of properties)
   * @return String array that can be used to run tests
   */
  public String[] build() {
    List<String> result = new ArrayList<>();
    if (withCommonHadoopFlags) {
      CommonArgs.addHadoopFlags(result);
    }
    if (!properties.isEmpty()) {
      Collections.addAll(result, createArgumentArrayFromProperties(properties.values()));
    }
    if (!options.isEmpty()) {
      Collections.addAll(result, createArgumentArrayFromOptions(options.values()));
    }
    if (!toolOptions.isEmpty()) {
      result.add(TOOL_ARG_SEPARATOR);
      Collections.addAll(result, createArgumentArrayFromOptions(toolOptions.values()));
    }
    return result.toArray(new String[result.size()]);
  }

  private String[] createArgumentArrayFromProperties(Iterable<Argument> properties) {
    List<String> result = new ArrayList<>();
    for (Argument property : properties) {
      result.add(PROPERTY_PREFIX);
      result.add(property.toString());
    }
    return result.toArray(new String[result.size()]);
  }

  private String[] createArgumentArrayFromOptions(Iterable<Argument> options) {
    List<String> result = new ArrayList<>();
    for (Argument option : options) {
      result.add(OPTION_PREFIX + option.getName());
      if (!isEmpty(option.getValue())) {
        result.add(option.getValue());
      }
    }
    return result.toArray(new String[result.size()]);
  }

  @Override
  public String toString() {
    return Arrays.toString(build());
  }
}