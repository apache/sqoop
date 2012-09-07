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
package org.apache.sqoop.client.utils;

import jline.ConsoleReader;
import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MStringInput;
import org.codehaus.groovy.tools.shell.IO;

import java.io.IOException;
import java.util.ResourceBundle;

/**
 *
 */
public class FormFiller {


  public static boolean fillForms(IO io,
                                  MConnectionForms formsMetadata,
                                  ConsoleReader reader,
                                  ResourceBundle bundle)
    throws IOException {
    for (MForm form : formsMetadata.getForms()) {
      if(!fillForm(io, form, reader, bundle)) {
        return false;
      }
    }

    return true;
  }

  public static boolean fillForm(IO io,
                                 MForm form,
                                 ConsoleReader reader,
                                 ResourceBundle bundle) throws IOException {
    io.out.println("");
    io.out.println(bundle.getString(form.getLabelKey()));
    for (MInput input : form.getInputs()) {
      if(!fillInput(io, input, reader, bundle)) {
        return false;
      }
    }

    return true;
  }

  public static boolean fillInput(IO io,
                                  MInput input,
                                  ConsoleReader reader,
                                  ResourceBundle bundle) throws IOException {
    // Print out warning or error message in case some validations were already
    // performed.
    switch (input.getValidationSeverity()) {
      case ERROR:
        io.out.println("Error message: @|red "
          + input.getValidationMessage() + " |@");
        break;
      case WARNING:
        io.out.println("Warning message: @|yellow "
          + input.getValidationMessage() + " |@");
        break;
      default:
        // Simply ignore all other states for the moment
        break;
    }

    // Based on the input type, let's perform specific load
    switch (input.getType()) {
      case STRING:
        return fillInputString(io, (MStringInput) input, reader, bundle);
      //TODO(jarcec): Support MAP
      default:
        io.out.println("Unsupported data type " + input.getType());
        return true;
    }
  }

  public static boolean fillInputString(IO io,
                                        MStringInput input,
                                        ConsoleReader reader,
                                        ResourceBundle bundle)
                                        throws IOException {
    // Print prompt
    reader.printString(bundle.getString(input.getLabelKey()) + ": ");
    reader.flushConsole();

    // Fill already filled data when available
    if(!input.isEmpty()) {
      reader.putString(input.getValue());
    }

    // Get the data
    String userTyped;
    if(input.isMasked()) {
       userTyped = reader.readLine('*');
    } else {
      userTyped = reader.readLine();
    }

    if (userTyped == null) {
      return false;
    } else if (userTyped.isEmpty()) {
      input.setEmpty();
    } else {
      input.setValue(userTyped);
    }

    return true;
  }

  private FormFiller() {
    // Do not instantiate
  }
}
