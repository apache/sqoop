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

package org.apache.sqoop;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.sqoop.tool.ImportAllTablesTool;
import com.cloudera.sqoop.tool.SqoopTool;
import org.apache.sqoop.validation.AbsoluteValidationThreshold;
import org.assertj.core.api.SoftAssertions;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class TestSqoopOptions {

  private Random random = new Random();

  public static final String COLUMN_MAPPING = "test=INTEGER,test1=DECIMAL(1%2C1),test2=NUMERIC(1%2C%202)";

  private Set<Class> excludedClassesFromClone = new HashSet<>();
  private Set<String> excludedFieldsFromClone = new HashSet<>();

  @Before
  public void setUp() {
    excludedClassesFromClone.add(String.class);
    excludedClassesFromClone.add(Class.class);
    excludedClassesFromClone.add(Integer.class);

    excludedFieldsFromClone.add("parent");
    excludedFieldsFromClone.add("incrementalMode");
    excludedFieldsFromClone.add("updateMode");
    excludedFieldsFromClone.add("layout");
    excludedFieldsFromClone.add("activeSqoopTool");
  }

  @Test
  public void testParseColumnParsing() {
    new SqoopOptions() {
  @Test
      public void testParseColumnMapping() {
        Properties result = new Properties();
        parseColumnMapping(COLUMN_MAPPING, result);
        assertEquals("INTEGER", result.getProperty("test"));
        assertEquals("DECIMAL(1,1)", result.getProperty("test1"));
        assertEquals("NUMERIC(1, 2)", result.getProperty("test2"));
      }
    }.testParseColumnMapping();
  }

  @Test
  public void testColumnNameCaseInsensitive() {
    SqoopOptions opts = new SqoopOptions();
    opts.setColumns(new String[]{ "AAA", "bbb" });
    assertEquals("AAA", opts.getColumnNameCaseInsensitive("aAa"));
    assertEquals("bbb", opts.getColumnNameCaseInsensitive("BbB"));
    assertEquals(null, opts.getColumnNameCaseInsensitive("notFound"));
    opts.setColumns(null);
    assertEquals(null, opts.getColumnNameCaseInsensitive("noColumns"));
  }

  @Test
  public void testSqoopOptionsCloneIsEqual() throws Exception {
    SqoopOptions options = createSqoopOptionsFilledWithRandomData();
    SqoopOptions clonedOptions = (SqoopOptions) options.clone();
    assertThat(options).isEqualToComparingFieldByFieldRecursively(clonedOptions);
  }

  @Test
  public void testSqoopOptionsCloneHasDistinctReferenceTypes() throws Exception{
    SqoopOptions options = createSqoopOptionsFilledWithRandomData();
    SqoopOptions clonedOptions = (SqoopOptions) options.clone();
    SoftAssertions softly = new SoftAssertions();

    for(Field field : getEligibleFields(options.getClass())) {
      softly.assertThat(field.get(clonedOptions))
        .describedAs(String.format("%s : %s", field.getName(), field.getType()))
        .isNotSameAs(field.get(options));
    }
    softly.assertAll();
  }

  private Iterable<? extends Field> getEligibleFields(Class<? extends SqoopOptions> clazz) {
    Field[] allFields = FieldUtils.getAllFields(clazz);
    List<Field> eligibleFields = new ArrayList<>();
    for(Field field : allFields) {
      if(!field.getType().isPrimitive()
              && !excludedClassesFromClone.contains(field.getType())
              && !excludedFieldsFromClone.contains(field.getName())
              && !Modifier.isStatic(field.getModifiers())
              && !Modifier.isFinal(field.getModifiers())) { // final and static fields are expected to be the same
        field.setAccessible(true);
        eligibleFields.add(field);
      }
    }
    return eligibleFields;
  }

  private SqoopOptions createSqoopOptionsFilledWithRandomData() throws  Exception {
    SqoopOptions options;
    options = new SqoopOptions();
    options.setMapColumnJava(COLUMN_MAPPING);
    options.getColumnNames(); // initializes the mapReplacedColumnJava field, which is cloned but is otherwise inaccessible

    Field[] allFields = FieldUtils.getAllFields(options.getClass());
    for (Field field: allFields) {
      setFieldToValueIfApplicable(options, field);
    }
    return options;
  }

  private void setFieldToValueIfApplicable(Object target, Field field) throws Exception {
    int modifiers = field.getModifiers();
    if(!Modifier.isFinal(modifiers) && !Modifier.isAbstract(modifiers) && !Modifier.isStatic(modifiers)) {
      field.setAccessible(true);
      field.set(target, getValueForField(field));
    }
  }

  private <T> T createAndFill(Class<T> clazz) throws Exception {
    T instance = clazz.newInstance();
    for(Field field: clazz.getDeclaredFields()) {
      if (field.getType().equals(clazz)
              || field.getType().equals(ClassLoader.class)
              ) { // need to protect against circles.
        continue;
      }
      setFieldToValueIfApplicable(instance, field);
    }
    return instance;
  }

  private Object getValueForField(Field field) throws Exception {
    Class<?> type = field.getType();

    // This list is not complete! Add new types as needed
    if(type.isEnum()) {
      Object[] enumValues = type.getEnumConstants();
      return enumValues[random.nextInt(enumValues.length)];
    }
    else if(type.equals(Integer.TYPE) || type.equals(Integer.class)) {
      return random.nextInt();
    }
    else if(type.equals(Long.TYPE) || type.equals(Long.class)) {
      return random.nextLong();
    }
    else if(type.equals(Double.TYPE) || type.equals(Double.class)) {
      return random.nextDouble();
    }
    else if(type.equals(Float.TYPE) || type.equals(Float.class)) {
      return random.nextFloat();
    }
    else if(type.equals(String.class)) {
      return UUID.randomUUID().toString();
    }
    else if(type.equals(BigInteger.class)){
      return BigInteger.valueOf(random.nextInt());
    }
    else if(type.isAssignableFrom(HashMap.class)) {
      HashMap<String, String> map = new HashMap<>();
      map.put("key1", "value1");
      map.put("key2", "value2");
      map.put("key3", "value3");
      return map;
    }
    else if(type.equals(Set.class)) {
      Set<String> set = new HashSet<>();
      set.add("value1");
      set.add("value2");
      set.add("value3");
      return set;
    }
    else if (type.isArray()) {
      int length = random.nextInt(9) + 1;
      return Array.newInstance(type.getComponentType(), length);
    }
    else if (Number.class.isAssignableFrom(type)) {
      return random.nextInt(Byte.MAX_VALUE) + 1;
    }
    else if(type.equals(boolean.class)) {
      return random.nextBoolean();
    }
    else if(SqoopTool.class.equals(field.getType())) {
      return new ImportAllTablesTool();
    }
    else if (field.getType().equals(ArrayList.class)) {
      return new ArrayList<>();
    } else if (field.getType().equals(Class.class)) {
      return AbsoluteValidationThreshold.class;
    }
    return createAndFill(type);
  }
}
