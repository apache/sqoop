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
package org.apache.sqoop.metastore;

import org.apache.sqoop.testcategories.sqooptest.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

import static org.apache.sqoop.SqoopOptions.DB_PASSWORD_KEY;
import static org.apache.sqoop.metastore.PasswordRedactor.REDACTED_PASSWORD_STRING;
import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class PasswordRedactorTest {

    @Test
    public void testRedactValueWithPasswordFieldReturnsRedactedValue() {
        assertEquals(REDACTED_PASSWORD_STRING, PasswordRedactor.redactValue(DB_PASSWORD_KEY, "secret"));
    }

    @Test
    public void testRedactValueWithNonPasswordFieldReturnsInputValue() {
        String nonPasswordFieldKey = "non_password_field";
        String inputValue = "not_a_secret";
        assertEquals(inputValue, PasswordRedactor.redactValue(nonPasswordFieldKey, inputValue));
    }

    @Test
    public void testRedactValuesRedactsPasswordFieldAndDoesNotChangeTheOthers() {
        String nonPasswordFieldKey = "non_password_field";
        String nonPasswordFieldValue = "non_password_value";
        Map<String, String> input = new HashMap<>();
        input.put(nonPasswordFieldKey, nonPasswordFieldValue);
        input.put(DB_PASSWORD_KEY, "secret");

        Map<String, String> expected = new HashMap<>();
        expected.put(nonPasswordFieldKey, nonPasswordFieldValue);
        expected.put(DB_PASSWORD_KEY, REDACTED_PASSWORD_STRING);

        assertEquals(expected, PasswordRedactor.redactValues(input));
    }

}