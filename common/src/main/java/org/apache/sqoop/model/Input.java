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
package org.apache.sqoop.model;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Field annotation. Each field that user might change in configuration object
 * need to have this annotation.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Input {
  /**
   * Sqoop will ensure that sensitive information will not be easily
   * accessible.
   *
   * @return True if field is sensitive
   */
  boolean sensitive() default false;

  /**
   * Indicates the entity that can edit the input's values, all inputs are
   * created/deleted only by the connector code, other entities do not have
   * access to either create/delete an input
   *
   * @return editable
   */
  InputEditable editable() default InputEditable.ANY;

  /**
   * Maximal length of field if applicable.
   *
   * @return Maximal length
   */
  short size() default -1;

  /**
   * In-order to express dependency on other inputs, the value supports a comma
   * separated list of other inputs in the config class. It validates the
   * attribute value obeys the expected conditions
   */
  String overrides() default "";

  /**
   * List of validators associated with this input.
   *
   * @return
   */
  Validator[] validators() default {};

}
