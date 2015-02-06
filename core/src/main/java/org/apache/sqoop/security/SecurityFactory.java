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
package org.apache.sqoop.security;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.utils.ClassUtils;

/**
 * Create authentication manager.
 */
public class SecurityFactory {

  public static AuthenticationHandler getAuthenticationHandler(String handler) throws ClassNotFoundException, IllegalAccessException, InstantiationException {

    Class<?> handlerClass = ClassUtils.loadClass(handler);

    if (handlerClass == null) {
      throw new SqoopException(SecurityError.AUTH_0004,
              "Authentication Handler Class is null: " + handler);
    }

    AuthenticationHandler newHandler;
    try {
      newHandler = (AuthenticationHandler) handlerClass.newInstance();
    } catch (Exception ex) {
      throw new SqoopException(SecurityError.AUTH_0004,
              "Authentication Handler Class Exception: " + handler, ex);
    }
    return newHandler;
  }

  public static AuthorizationHandler getAuthorizationHandler(String handler) throws ClassNotFoundException, IllegalAccessException, InstantiationException {

    Class<?> handlerClass = ClassUtils.loadClass(handler);

    if (handlerClass == null) {
      throw new SqoopException(SecurityError.AUTH_0007,
              "Authorization Handler Class is null: " + handler);
    }

    AuthorizationHandler newHandler;
    try {
      newHandler = (AuthorizationHandler) handlerClass.newInstance();
    } catch (Exception ex) {
      throw new SqoopException(SecurityError.AUTH_0007,
              "Authorization Handler Class Exception: " + handler, ex);
    }
    return newHandler;
  }

  public static AuthorizationAccessController getAuthorizationAccessController(String accessController) throws ClassNotFoundException, IllegalAccessException, InstantiationException {

    Class<?> accessControllerClass = ClassUtils.loadClass(accessController);

    if (accessControllerClass == null) {
      throw new SqoopException(SecurityError.AUTH_0008,
              "Authorization Access Controller Class is null: " + accessController);
    }

    AuthorizationAccessController newAccessController;
    try {
      newAccessController = (AuthorizationAccessController) accessControllerClass.newInstance();
    } catch (Exception ex) {
      throw new SqoopException(SecurityError.AUTH_0008,
              "Authorization Access Controller Class Exception: " + accessController, ex);
    }
    return newAccessController;
  }

  public static AuthorizationValidator getAuthorizationValidator(String validator) throws ClassNotFoundException, IllegalAccessException, InstantiationException {

    Class<?> validatorClass = ClassUtils.loadClass(validator);

    if (validatorClass == null) {
      throw new SqoopException(SecurityError.AUTH_0009,
              "Authorization Validator Class is null: " + validator);
    }

    AuthorizationValidator newValidator;
    try {
      newValidator = (AuthorizationValidator) validatorClass.newInstance();
    } catch (Exception ex) {
      throw new SqoopException(SecurityError.AUTH_0009,
              "Authorization Validator Class Exception: " + validator, ex);
    }
    return newValidator;
  }

  public static AuthenticationProvider getAuthenticationProvider(String provider) throws ClassNotFoundException, IllegalAccessException, InstantiationException {

    Class<?> providerClass = ClassUtils.loadClass(provider);

    if (providerClass == null) {
      throw new SqoopException(SecurityError.AUTH_0010,
              "Authentication Provider Class is null: " + provider);
    }

    AuthenticationProvider newProvider;
    try {
      newProvider = (AuthenticationProvider) providerClass.newInstance();
    } catch (Exception ex) {
      throw new SqoopException(SecurityError.AUTH_0010,
              "Authentication Provider Class is null: " + provider, ex);
    }
    return newProvider;
  }
}