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
package org.apache.sqoop.utils;

import org.apache.log4j.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public final class ClassUtils {

  private static final Logger LOG = Logger.getLogger(ClassUtils.class);

  /**
   * Load class by given name and return corresponding Class object.
   *
   * This method will return null in case that the class is not found, no
   * exception will be rised.
   *
   * @param className Name of class
   * @return Class instance or NULL
   */
  public static Class<?> loadClass(String className) {
    Class<?> klass = null;
    try {
      klass = Class.forName(className);
    } catch (ClassNotFoundException ex) {
      LOG.debug("Exception while loading class: " + className, ex);
    }

    if (klass == null) {
      // Try the context class loader if one exists
      ClassLoader ctxLoader = Thread.currentThread().getContextClassLoader();
      if (ctxLoader != null) {
        try {
          klass = ctxLoader.loadClass(className);
        } catch (ClassNotFoundException ex) {
          LOG.debug("Exception while load class: " + className, ex);
        }
      }
    }

    return klass;
  }

  /**
   * Create instance of given class and given parameters.
   *
   * Please note that due to inherited limitations from Java languge, this
   * method can't handle primitive types and NULL values.
   *
   * @param className Class name
   * @param args Objects that should be passed as constructor arguments.
   * @return Instance of new class or NULL in case of any error
   */
  public static Object instantiate(String className, Object ... args) {
    return instantiate(loadClass(className), args);
  }

  /**
   * Create instance of given class and given parameters.
   *
   * Please note that due to inherited limitations from Java languge, this
   * method can't handle primitive types and NULL values.
   *
   * @param klass Class object
   * @param args Objects that should be passed as constructor arguments.
   * @return Instance of new class or NULL in case of any error
   */
  public static Object instantiate(Class klass, Object ... args) {
    if(klass == null) {
      return null;
    }

    Class []argumentTypes = new Class[args.length];
    for(int i = 0; i < args.length; i++) {
      Class type = args[i].getClass();
       argumentTypes[i] = type;
    }

    try {
      Constructor constructor = klass.getConstructor(argumentTypes);
      return constructor.newInstance(args);
    } catch (NoSuchMethodException e) {
      LOG.error("Can't find such constructor.", e);
    } catch (InvocationTargetException e) {
      LOG.error("Can't instantiate object.", e);
    } catch (InstantiationException e) {
      LOG.error("Can't instantiate object.", e);
    } catch (IllegalAccessException e) {
      LOG.error("Can't instantiate object.", e);
    }

    return null;
  }

  /**
   * Return jar path for given class.
   *
   * @param className Class name
   * @return Path on local filesystem to jar where given jar is present
   */
  public static String jarForClass(String className) {
    Class klass = loadClass(className);
    return klass.getProtectionDomain().getCodeSource().getLocation().toString();
  }


  /**
   * Return jar path for given class.
   *
   * @param klass Class object
   * @return Path on local filesystem to jar where given jar is present
   */
  public static String jarForClass(Class klass) {
    return klass.getProtectionDomain().getCodeSource().getLocation().toString();
  }

  private ClassUtils() {
    // Disable explicit object creation
  }
}
