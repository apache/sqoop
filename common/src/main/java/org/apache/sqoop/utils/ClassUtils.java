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

import java.lang.ref.WeakReference;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.log4j.Logger;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class ClassUtils {

  private static final Logger LOG = Logger.getLogger(ClassUtils.class);

  private static final Map<ClassLoader, Map<String, WeakReference<Class<?>>>>
    CACHE_CLASSES = new WeakHashMap<ClassLoader, Map<String, WeakReference<Class<?>>>>();

  /**
   * Sentinel value to store negative cache results in {@link #CACHE_CLASSES}.
   */
  private static final Class<?> NEGATIVE_CACHE_SENTINEL =
    NegativeCacheSentinel.class;

  private static ClassLoader defaultClassLoader;
  static {
    defaultClassLoader = Thread.currentThread().getContextClassLoader();
    if (defaultClassLoader == null) {
      defaultClassLoader = ClassUtils.class.getClassLoader();
    }
  }

  /**
   * A unique class which is used as a sentinel value in the caching
   * for loadClass.
   */
  private static abstract class NegativeCacheSentinel {}

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
    return loadClassWithClassLoader(className, defaultClassLoader);
  }

  /**
   * Load class by given name and classLoader.
   *
   * This method will return null in case that the class is not found, no
   * exception will be rised.
   *
   * @param className Name of class
   * @param loader classLoader to load the given class
   * @return Class instance or NULL
   */
  public static Class<?> loadClassWithClassLoader(String className, ClassLoader loader) {
    if(className == null) {
      return null;
    }

    Map<String, WeakReference<Class<?>>> map;
    synchronized (CACHE_CLASSES) {
      map = CACHE_CLASSES.get(loader);
      if (map == null) {
        map = Collections.synchronizedMap(
          new WeakHashMap<String, WeakReference<Class<?>>>());
        CACHE_CLASSES.put(loader, map);
      }
    }

    Class<?> klass = null;
    WeakReference<Class<?>> ref = map.get(className);
    if (ref != null) {
      klass = ref.get();
    }

    if (klass == null) {
      try {
        klass = Class.forName(className, true, loader);
      } catch (ClassNotFoundException ex) {
        // Leave a marker that the class isn't found
        map.put(className, new WeakReference<Class<?>>(NEGATIVE_CACHE_SENTINEL));
        LOG.debug("Exception while loading class: " + className, ex);
        return null;
      }
      // two putters can race here, but they'll put the same class
      map.put(className, new WeakReference<Class<?>>(klass));
      return klass;
    } else if (klass == NEGATIVE_CACHE_SENTINEL) {
      return null; // not found
    } else {
      // cache hit
      return klass;
    }
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
    return instantiateWithClassLoader(className, defaultClassLoader, args);
  }

  /**
   * Create instance of given class and given parameters.
   *
   * @param className Class name
   * @param loader classLoader to load the given class
   * @param args Objects that should be passed as constructor arguments
   * @return Instance of new class or NULL in case of any error
   */
  public static Object instantiateWithClassLoader(String className,
      ClassLoader loader, Object... args) {
    return instantiate(loadClassWithClassLoader(className, loader), args);
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

    Constructor []constructors = klass.getConstructors();

    for (Constructor constructor : constructors) {
      try {
        return constructor.newInstance(args);
      } catch (InvocationTargetException e) {
        LOG.debug("Can't instantiate object.", e);
      } catch (InstantiationException e) {
        LOG.trace("Can't instantiate object.", e);
      } catch (IllegalAccessException e) {
        LOG.trace("Can't instantiate object.", e);
      } catch (IllegalArgumentException e) {
        LOG.trace("Can't instantiate object.", e);
      }
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
    return jarForClassWithClassLoader(className, defaultClassLoader);
  }

  /**
   * Return jar path for given class.
   *
   * @param className Class name
   * @param loader classLoader to load the given class
   * @return Path on local filesystem to jar where given jar is present
   */
  public static String jarForClassWithClassLoader(String className, ClassLoader loader) {
    Class klass = loadClassWithClassLoader(className, loader);
    return jarForClass(klass);
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

  /**
   * Get list of constants from given Enum as a array of strings.
   *
   * @param klass Enumeration class
   * @return Array of string representation or null in case of any error
   */
  public static String[] getEnumStrings(Class klass) {
    if(!klass.isEnum()) {
      return new String[]{};
    }

    ArrayList<String> values = new ArrayList<String>();

    try {
      Method methodName = klass.getMethod("name");

      for(Object constant : klass.getEnumConstants()) {
        values.add((String) methodName.invoke(constant));
      }
    } catch (RuntimeException|NoSuchMethodException|IllegalAccessException|InvocationTargetException e) {
      LOG.error("Can't get list of values from enumeration " + klass.getCanonicalName(), e);
      return new String[]{};
    }

    return values.toArray(new String[values.size()]);
  }

  private ClassUtils() {
    // Disable explicit object creation
  }
}
