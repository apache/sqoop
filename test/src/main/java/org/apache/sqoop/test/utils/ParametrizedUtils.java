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
package org.apache.sqoop.test.utils;

import org.apache.commons.lang.ArrayUtils;

import java.util.LinkedList;
import java.util.List;

/**
 * Various utils to help build expected structure for JUNIT Parametrized runner.
 */
public class ParametrizedUtils {

  /**
   * Join two arrays of parameters by providing their cross product to make
   * testing matrix for Parameterized runner easier.
   *
   * This method accepts both arguments as array of arrays. In such case
   * the inner array will be flattened during the join to meet the Parametrized
   * runner requirements.
   *
   * @param v1 First array of test values
   * @param v2 Second array of test values
   * @return Cross product of v1 and v2
   */
  public static Iterable<Object []> crossProduct(Object[] v1, Object[] v2) {
    LinkedList<Object []> ret = new LinkedList<Object []>();

    for(Object o1 : v1) {
      for(Object o2 : v2) {
        ret.add(mergeObjects(o1, o2));
      }
    }

    return ret;
  }

  /**
   * Convert single array to array of arrays by putting each element in the source
   * array into it's own one-item big array.
   *
   * This method is suitable for conversions required by Parametrized test runner.
   *
   * @param array Array to be converted
   * @return Converted array
   */
  public static Iterable<Object []>toArrayOfArrays(Object []array) {
    LinkedList<Object []> ret = new LinkedList<Object []>();

    for(Object o : array) {
      ret.add(toArray(o));
    }

    return ret;
  }

  /**
   * Convert single list to array of arrays by putting each element in the source
   * list into it's own one-item big array.
   *
   * This method is suitable for conversions required by Parametrized test runner.
   *
   * @param list List to be converted
   * @return Converted array
   */
  public static Iterable<Object []>toArrayOfArrays(List<? extends Object> list) {
     LinkedList<Object []> ret = new LinkedList<Object []>();

    for(Object o : list) {
      ret.add(toArray(o));
    }

    return ret;
  }

  /**
   * Merge two objects into array.
   *
   * This method will flatten any argument that is already an array, e.g.:
   *
   * mergeObjects(1, 2) = [1, 2]
   * mergeObjects([1], 2) = [1, 2]
   * mergeObjects(1, [2]) = [1, 2]
   * mergeObjects([1], [2]) = [1, 2]
   *
   * @param o1
   * @param o2
   * @return
   */
  public static Object [] mergeObjects(Object o1, Object o2) {
    if(!o1.getClass().isArray() && !o2.getClass().isArray()) {
      return new Object[] { o1, o2 };
    }

    Object []a1 = toArray(o1);
    Object []a2 = toArray(o2);

    return ArrayUtils.addAll(a1, a2);
  }

  /**
   * Convert object into one element array if it's not an array already.
   *
   * @param o
   * @return
   */
  public static Object[] toArray(Object o) {
    if(o.getClass().isArray()) {
      return (Object [])o;
    }

    return new Object[] { o };
  }
}
