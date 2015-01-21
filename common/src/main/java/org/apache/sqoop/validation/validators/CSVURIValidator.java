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

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.validation.Status;

/**
 * Validate a string takes on the form: host:port,host:port,...
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class CSVURIValidator extends AbstractValidator<String> {

  // validate that given string is a comma-separated list of host:port
  @Override
  public void validate(String str) {
    if(str != null && str != "") {
      String[] pairs = str.split("\\s*,\\s*");
      for (String pair: pairs) {
        String[] parts = pair.split("\\s*:\\s*");
        if (parts.length != 2) {
          addMessage(Status.ERROR,"can't parse into host:port pairs");
        } else {
          String[] rightParts = parts[1].split("/");

          try {
            int port = Integer.parseInt(rightParts[0]);
            if (port < 0) {
              addMessage(Status.ERROR, "Can't parse port less than 0");
            } else if (port > 65535) {
              addMessage(Status.ERROR, "Can't parse port greater than 65535");
            }
          } catch(NumberFormatException e) {
            addMessage(Status.ERROR, "Can't parse port");
          }
        }
      }
    } else {
      addMessage(Status.ERROR, "Can't be null nor empty");
    }
  }
}