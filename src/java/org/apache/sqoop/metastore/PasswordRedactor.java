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

import static org.apache.sqoop.SqoopOptions.DB_PASSWORD_KEY;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class PasswordRedactor {

    static final String REDACTED_PASSWORD_STRING = "********";

    private static final Collection<String> REDACTION_KEYS = Arrays.asList(DB_PASSWORD_KEY);

    public static String redactValue(String key, String value) {
        if (REDACTION_KEYS.contains(key)) {
            return REDACTED_PASSWORD_STRING;
        } else {
            return value;
        }
    }

    public static Map<String, String> redactValues(Map<?, ?> values) {
        Map<String, String> result = new HashMap<>();

        for (Map.Entry<?, ?> entry : values.entrySet()) {
            String key = entry.getKey().toString();
            result.put(key, redactValue(key, entry.getValue().toString()));
        }

        return result;
    }

}
