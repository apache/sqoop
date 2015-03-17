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
package org.apache.sqoop.validation.validators;

import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.validation.Status;

import java.io.File;

/**
 * Verify that given directory exist on the Sqoop 2 server machine.
 */
public class DirectoryExistsValidator extends AbstractValidator<String> {
  @Override
  public void validate(String filePath) {
    if(StringUtils.isBlank(filePath)) {
      return;
    }

    File file = new File(filePath);

    if(!file.exists()) {
      addMessage(Status.ERROR, "Directory doesn't exists on Sqoop 2 server machine");
      return;
    }

    if(!file.isDirectory()) {
      addMessage(Status.ERROR, "Path is not a valid directory");
      return;
    }
  }
}
