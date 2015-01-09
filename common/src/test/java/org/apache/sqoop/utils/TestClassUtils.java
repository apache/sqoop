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

import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertArrayEquals;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

/**
 *
 */
public class TestClassUtils {

  @Test
  public void testLoadClass() {
    assertNull(ClassUtils.loadClass("A"));
    assertEquals(A.class, ClassUtils.loadClass(A.class.getName()));
  }

  @Test
  public void testInstantiateNull() {
    assertNull(ClassUtils.instantiate((Class) null));
  }

  @Test
  public void testInstantiate() {
    // Just object calls
    A a = (A) ClassUtils.instantiate(A.class, "a");
    assertNotNull(a);
    assertEquals(1, a.num);
    assertEquals("a", a.a);

    // Automatic wrapping primitive -> objects
    A b = (A) ClassUtils.instantiate(A.class, "b", 3, 5);
    assertNotNull(b);
    assertEquals(3, b.num);
    assertEquals("b", b.a);
    assertEquals(3, b.b);
    assertEquals(5, b.c);

    // Primitive types in the constructor definition
    Primitive p = (Primitive) ClassUtils.instantiate(Primitive.class, 1, 1.0f, true);
    assertNotNull(p);
    assertEquals(1, p.i);
    assertEquals(1.0f, p.f, 0.0f);
    assertEquals(true, p.b);

    // Subclasses can be used in the constructor call
    A c = (A) ClassUtils.instantiate(A.class, new Child());
    assertNotNull(c);
    assertNotNull(c.p);
    assertEquals(Child.class, c.p.getClass());
  }

  public static class Parent {
  }

  public static class Child extends Parent {
  }


  public static class A {
    String a;
    int b;
    int c;
    int num;
    Parent p;

    public A(String a) {
      num = 1;
      this.a = a;
    }
    public A(String a, Integer b, Integer c) {
      this(a);

      num = 3;
      this.b = b;
      this.c = c;
    }

    public A(Parent p) {
      this.p = p;
    }
  }

  public static class Primitive {
    int i;
    float f;
    boolean b;

    public Primitive(int i, float f, boolean b) {
      this.i = i;
      this.f = f;
      this.b = b;
    }
  }

  @Test
  public void testGetEnumStrings() {
    assertNull(ClassUtils.getEnumStrings(A.class));

    assertArrayEquals(
      new String[]{"A", "B", "C"},
      ClassUtils.getEnumStrings(EnumA.class)
    );
    assertArrayEquals(
      new String[]{"X", "Y"},
      ClassUtils.getEnumStrings(EnumX.class)
    );
  }

  enum EnumX {
    X, Y
  }

  enum EnumA {
    A, B, C
  }
}
