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
package org.apache.sqoop.common;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.sqoop.json.JsonBean;
import org.json.simple.JSONObject;

public class ExceptionInfo implements JsonBean {

  public static final String ERROR_CODE = "error-code";
  public static final String ERROR_MESSAGE = "error-message";
  public static final String STACK_TRACE = "stack-trace";

  private String errorCode;
  private String errorMessage;
  private String stackTrace;

  public ExceptionInfo(String code, String message, Exception ex) {
    errorCode = code;
    errorMessage = message;

    StringWriter writer = new StringWriter();
    ex.printStackTrace(new PrintWriter(writer));
    writer.flush();

    stackTrace = writer.getBuffer().toString();
  }

  @SuppressWarnings("unchecked")
  @Override
  public JSONObject extract() {
    JSONObject result = new JSONObject();
    result.put(ERROR_CODE, errorCode);
    result.put(ERROR_MESSAGE, errorMessage);
    result.put(STACK_TRACE, stackTrace);

    return result;
  }

  @Override
  public void restore(JSONObject jsonObject) {
    errorCode = (String) jsonObject.get(ERROR_CODE);
    errorMessage = (String) jsonObject.get(ERROR_MESSAGE);
    stackTrace = (String) jsonObject.get(STACK_TRACE);
  }
}
