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

import junit.framework.TestCase;

/**
 *
 */
public class TestClassUtils extends TestCase {

//  public void testLoadClass() {
//    assertNull(ClassUtils.loadClass("A"));
//    assertEquals(A.class, ClassUtils.loadClass(A.class.getName()));
//  }
//
//  public void testInstantiateNull() {
//    assertNull(ClassUtils.instantiate((Class) null));
//  }
//
//  public void testInstantiate() {
//    A a = (A) ClassUtils.instantiate(A.class, "a");
//    assertNotNull(a);
//    assertEquals(1, a.num);
//    assertEquals("a", a.a);
//
//    A b = (A) ClassUtils.instantiate(A.class, "b", 3, 5);
//    assertNotNull(b);
//    assertEquals(3, b.num);
//    assertEquals("b", b.a);
//    assertEquals(3, b.b);
//    assertEquals(5, b.c);
//  }
//
//  public static class A {
//    String a;
//    int b;
//    int c;
//    int num;
//
//    public A(String a) {
//      num = 1;
//      this.a = a;
//    }
//    public A(String a, Integer b, Integer c) {
//      this(a);
//
//      num = 3;
//      this.b = b;
//      this.c = c;
//    }
//  }
//
//  public void testGetEnumStrings() {
//    assertNull(ClassUtils.getEnumStrings(A.class));
//
//    assertEquals(
//      new String[] {"A", "B", "C"},
//      ClassUtils.getEnumStrings(EnumA.class)
//    );
//    assertEquals(
//      new String[] {"X", "Y"},
//      ClassUtils.getEnumStrings(EnumX.class)
//    );
//  }
//
//  enum EnumX {
//    X, Y
//  }
//
//  enum EnumA {
//    A, B, C
//  }
//
//  public void assertEquals(String[] expected, String[] actual) {
//    assertEquals("Arrays do not have same length", expected.length, actual.length);
//
//    for(int i = 0; i < expected.length; i++) {
//      assertEquals("Items on position " + i + " differs, expected "
//        + expected[i] + ", actual " + actual[i],
//        expected[i], actual[i]);
//    }
//  }
}
