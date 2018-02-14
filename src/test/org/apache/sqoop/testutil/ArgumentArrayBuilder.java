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

import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.commons.lang3.StringUtils.isEmpty;

public class ArgumentArrayBuilder {

  private static final String PROPERTY_PREFIX = "-D";

  private static final String OPTION_PREFIX = "--";
  public static final String TOOL_ARG_SEPARATOR = "--";

  private List<Argument> properties;

  private List<Argument> options;

  private List<Argument> toolOptions;

  private boolean withCommonHadoopFlags;

  public ArgumentArrayBuilder() {
    properties = new ArrayList<>();
    options = new ArrayList<>();
    toolOptions = new ArrayList<>();
  }

  public ArgumentArrayBuilder withProperty(String name, String value) {
    properties.add(new Argument(name, value));
    return this;
  }

  public ArgumentArrayBuilder withProperty(String name) {
    properties.add(new Argument(name));
    return this;
  }

  public ArgumentArrayBuilder withOption(String name, String value) {
    options.add(new Argument(name, value));
    return this;
  }

  public ArgumentArrayBuilder withOption(String name) {
    options.add(new Argument(name));
    return this;
  }

  public ArgumentArrayBuilder withToolOption(String name, String value) {
    toolOptions.add(new Argument(name, value));
    return this;
  }

  public ArgumentArrayBuilder withToolOption(String name) {
    toolOptions.add(new Argument(name));
    return this;
  }

  public ArgumentArrayBuilder with(ArgumentArrayBuilder otherBuilder) {
    properties.addAll(otherBuilder.properties);
    options.addAll(otherBuilder.options);
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
    if (CollectionUtils.isNotEmpty(properties)) {
      Collections.addAll(result, createArgumentArrayFromProperties(properties));
    }
    if (CollectionUtils.isNotEmpty(options)) {
      Collections.addAll(result, createArgumentArrayFromOptions(options));
    }
    if (CollectionUtils.isNotEmpty(toolOptions)) {
      result.add(TOOL_ARG_SEPARATOR);
      Collections.addAll(result, createArgumentArrayFromOptions(toolOptions));
    }
    return result.toArray(new String[result.size()]);
  }

  private String[] createArgumentArrayFromProperties(List<Argument> properties) {
    List<String> result = new ArrayList<>();
    for (Argument property : properties) {
      result.add(PROPERTY_PREFIX);
      result.add(property.toString());
    }
    return result.toArray(new String[result.size()]);
  }

  private String[] createArgumentArrayFromOptions(List<Argument> options) {
    List<String> result = new ArrayList<>();
    for (Argument option : options) {
      result.add(OPTION_PREFIX + option.getName());
      if (!isEmpty(option.getValue())) {
        result.add(option.getValue());
      }
    }
    return result.toArray(new String[result.size()]);
  }
}