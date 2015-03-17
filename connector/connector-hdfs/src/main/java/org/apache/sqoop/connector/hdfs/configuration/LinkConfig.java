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
package org.apache.sqoop.connector.hdfs.configuration;

import org.apache.sqoop.model.ConfigClass;
import org.apache.sqoop.model.Input;
import org.apache.sqoop.model.Validator;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.validators.AbstractValidator;
import org.apache.sqoop.validation.validators.DirectoryExistsValidator;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@ConfigClass(validators = {@Validator(LinkConfig.ConfigValidator.class)})
public class LinkConfig {
  @Input(size = 255) public String uri;

  @Input(size = 255, validators = { @Validator(DirectoryExistsValidator.class)})
  public String confDir;

  public static class ConfigValidator extends AbstractValidator<LinkConfig> {
    private static final Pattern URI_PATTERN = Pattern.compile("((?<=\\()[A-Za-z][A-Za-z0-9\\+\\.\\-]*:([A-Za-z0-9\\.\\-_~:/\\?#\\[\\]@!\\$&'\\(\\)\\*\\+,;=]|%[A-Fa-f0-9]{2})+(?=\\)))|([A-Za-z][A-Za-z0-9\\+\\.\\-]*:([A-Za-z0-9\\.\\-_~:/\\?#\\[\\]@!\\$&'\\(\\)\\*\\+,;=]|%[A-Fa-f0-9]{2})+)");

    @Override
    public void validate(LinkConfig config) {
      if (config.uri != null) {
        Matcher matcher = URI_PATTERN.matcher(config.uri);
        if (!matcher.matches()) {
          addMessage(Status.ERROR,
              "Invalid URI" + config.uri + ". URI must either be null or a valid URI. Here are a few valid example URIs:"
              + " hdfs://example.com:8020/, hdfs://example.com/, file:///, file:///tmp, file://localhost/tmp");
        }
      }
    }
  }
}
