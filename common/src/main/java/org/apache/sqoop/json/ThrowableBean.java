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
package org.apache.sqoop.json;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.utils.ClassUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.LinkedList;
import java.util.List;

/**
 * Transfer throwable instance as a throwable bean.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ThrowableBean implements JsonBean {

  public static final String MESSAGE = "message";
  public static final String STACK_TRACE = "stack-trace";
  public static final String CLASS = "class";
  public static final String METHOD = "method";
  public static final String FILE = "file";
  public static final String LINE = "line";
  public static final String CAUSE = "cause";
  public static final String ERROR_CODE = "error-code";
  public static final String ERROR_CODE_CLASS = "error-code-class";

  private Throwable throwable;

  // For "extract"
  public ThrowableBean(Throwable ex) {
    throwable = ex;
  }

  // For "restore"
  public ThrowableBean() {
  }

  public Throwable getThrowable() {
    return throwable;
  }

  @Override
  @SuppressWarnings("unchecked")
  public JSONObject extract(boolean skipSensitive) {
    JSONObject result = new JSONObject();

    result.put(MESSAGE, throwable.getMessage());
    result.put(CLASS, throwable.getClass().getName());

    if(throwable instanceof SqoopException ) {
      SqoopException sqoopException = (SqoopException) throwable;
      result.put(ERROR_CODE, sqoopException.getErrorCode().getCode());
      result.put(ERROR_CODE_CLASS, sqoopException.getErrorCode().getClass().getName());
      // Override message with the original message
      result.put(MESSAGE, sqoopException.getOriginalMessage());
    }

    JSONArray st = new JSONArray();
    for(StackTraceElement element : throwable.getStackTrace()) {
      JSONObject obj = new JSONObject();

      obj.put(CLASS, element.getClassName());
      obj.put(METHOD, element.getMethodName());
      obj.put(FILE, element.getFileName());
      obj.put(LINE, element.getLineNumber());

      st.add(obj);
    }

    result.put(STACK_TRACE, st);

    Throwable cause = throwable.getCause();
    if(cause != null) {
      ThrowableBean causeBean = new ThrowableBean(cause);
      result.put(CAUSE, causeBean.extract(skipSensitive));
    }

    return result;
  }

  @Override
  public void restore(JSONObject jsonObject) {
    String exceptionClass = (String) jsonObject.get(CLASS);
    String message = (String) jsonObject.get(MESSAGE);
    if(message == null) {
      message = "";
    }

    // Special handling for SqoopException as we need to transfer ERROR_CODE from the other side
    if(jsonObject.containsKey(ERROR_CODE_CLASS)) {
      Class e = ClassUtils.loadClass((String) jsonObject.get(ERROR_CODE_CLASS));

      // Only if the error code class is known to this JVM, let's instantiate the real SqoopException
      if( e != null) {
        String errorCode = (String) jsonObject.get(ERROR_CODE);
        Enum enumValue = Enum.valueOf(e, errorCode);
        throwable = (Throwable) ClassUtils.instantiate(exceptionClass, enumValue, message);
      }
    }

    // Let's try to instantiate same class that was originally on remote side.
    if(throwable == null) {
      throwable = (Throwable) ClassUtils.instantiate(exceptionClass, message);
    }

    // Fallback to generic Throwable in case that this particular exception is not known
    // to this JVM (for example during  server-client exchange).
    if(throwable == null) {
      throwable = new Throwable(message);
    }

    List<StackTraceElement> st = new LinkedList<StackTraceElement>();
    for(Object object : (JSONArray)jsonObject.get(STACK_TRACE)) {
      JSONObject json = (JSONObject)object;
      StackTraceElement element = new StackTraceElement(
        (String)json.get(CLASS),
        (String)json.get(METHOD),
        (String)json.get(FILE),
        ((Long)json.get(LINE)).intValue()
      );
      st.add(element);
    }

    throwable.setStackTrace(st.toArray(new StackTraceElement[]{}));

    Object cause = jsonObject.get(CAUSE);
    if(cause != null) {
      JSONObject causeJson = (JSONObject)cause;

      ThrowableBean causeBean = new ThrowableBean();
      causeBean.restore(causeJson);

      throwable.initCause(causeBean.getThrowable());
    }
  }
}
