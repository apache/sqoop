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
package org.apache.sqoop.connector.jdbc;

import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.Validator;

import java.util.List;

import static org.apache.sqoop.connector.jdbc.GenericJdbcConnectorConstants.*;

/**
 *
 */
public class GenericJdbcValidator extends Validator {

  @Override
  public Status validate(MForm form) {
    if (form.getName().equals(FORM_CONNECTION)) {
      return validateConnectionForm(form);
    }

    if(form.getName().equals(FORM_TABLE)) {
      return Status.ACCEPTABLE;
    }

    // This do not seem as our form
    return Status.UNACCEPTABLE;
  }

  private Status validateConnectionForm(MForm form) {
    Status status = Status.FINE;

    List<MInput<?>> inputs = form.getInputs();

    for (MInput input : inputs) {
      // JDBC connection string must start with "jdbc:"
      if (input.getName().equals(INPUT_CONN_CONNECTSTRING)) {
        String jdbcUrl = (String) input.getValue();

        if(jdbcUrl == null || !jdbcUrl.startsWith("jdbc:")) {
          status = Status.UNACCEPTABLE;
          input.setErrorMessage("This do not seem as a valid JDBC URL.");
        }
      }
    }

    return status;
  }
}
