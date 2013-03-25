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
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MEnumInput;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MIntegerInput;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.model.MValidatedElement;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ResourceBundle;

import static org.apache.sqoop.client.shell.ShellEnvironment.*;

/**
 * Convenient methods for retrieving user input.
 */
public final class FormFiller {

  /**
   * Internal input that will be reused for loading names for connection and
   * job objects.
   */
  private static MStringInput nameInput = new MStringInput("object-name", false, (short)25);

  /**
   * Fill job object based on user input.
   *
   * @param reader Associated console reader object
   * @param job Job that user is suppose to fill in
   * @param connectorBundle Connector resource bundle
   * @param frameworkBundle Framework resource bundle
   * @return True if we filled all inputs, false if user has stopped processing
   * @throws IOException
   */
  public static boolean fillJob(ConsoleReader reader,
                                MJob job,
                                ResourceBundle connectorBundle,
                                ResourceBundle frameworkBundle)
                                throws IOException {

    job.setName(getName(reader, job.getName()));

    // Fill in data from user
     return fillForms(reader,
                      job.getConnectorPart().getForms(),
                      connectorBundle,
                      job.getFrameworkPart().getForms(),
                      frameworkBundle);
  }

  /**
   * Fill connection object based on user input.
   *
   * @param reader Associated console reader object
   * @param connection Connection that user is suppose to fill in
   * @param connectorBundle Connector resource bundle
   * @param frameworkBundle Framework resouce bundle
   * @return True if we filled all inputs, false if user has stopped processing
   * @throws IOException
   */
  public static boolean fillConnection(ConsoleReader reader,
                                       MConnection connection,
                                       ResourceBundle connectorBundle,
                                       ResourceBundle frameworkBundle)
                                       throws IOException {

    connection.setName(getName(reader, connection.getName()));

    // Fill in data from user
     return fillForms(reader,
                      connection.getConnectorPart().getForms(),
                      connectorBundle,
                      connection.getFrameworkPart().getForms(),
                      frameworkBundle);
  }

  public static boolean fillForms(ConsoleReader reader,
                                  List<MForm> connectorForms,
                                  ResourceBundle connectorBundle,
                                  List<MForm> frameworkForms,
                                  ResourceBundle frameworkBundle
                                  ) throws IOException {


    // Query connector forms
    if(!fillForms(connectorForms, reader, connectorBundle)) {
      return false;
    }

    // Query framework forms
    if(!fillForms(frameworkForms, reader, frameworkBundle)) {
      return false;
    }

    return true;
  }

  public static boolean fillForms(List<MForm> forms,
                                  ConsoleReader reader,
                                  ResourceBundle bundle)
    throws IOException {
    for (MForm form : forms) {
      if(!fillForm(form, reader, bundle)) {
        return false;
      }
    }

    return true;
  }

  public static boolean fillForm(MForm form,
                                 ConsoleReader reader,
                                 ResourceBundle bundle) throws IOException {
    println("");
    println(bundle.getString(form.getLabelKey()));

    // Print out form validation
    printValidationMessage(form);
    println("");

    for (MInput input : form.getInputs()) {
      if(!fillInput(input, reader, bundle)) {
        return false;
      }
    }

    return true;
  }

  public static boolean fillInput(MInput input,
                                  ConsoleReader reader,
                                  ResourceBundle bundle) throws IOException {
    // Print out validation
    printValidationMessage(input);

    // Based on the input type, let's perform specific load
    switch (input.getType()) {
      case STRING:
        return fillInputString((MStringInput) input, reader, bundle);
      case INTEGER:
        return fillInputInteger((MIntegerInput) input, reader, bundle);
      case MAP:
        return fillInputMap((MMapInput) input, reader, bundle);
      case ENUM:
        return fillInputEnum((MEnumInput) input, reader, bundle);
      default:
        println("Unsupported data type " + input.getType());
        return true;
    }
  }

  /**
   * Load user input for enum type.
   *
   * Print out numbered list of all available options and let user choose one
   * item from that.
   *
   * @param input Input that we should read or edit
   * @param reader Associated console reader
   * @param bundle Resource bundle
   * @return True if user with to continue with loading addtional inputs
   * @throws IOException
   */
  private static boolean fillInputEnum(MEnumInput input,
                                       ConsoleReader reader,
                                       ResourceBundle bundle)
                                       throws IOException {
    // Prompt in enum case
    println(bundle.getString(input.getLabelKey()) + ": ");

    // Indexes
    int i = -1;
    int lastChoice = -1;

    // Print out all values as a numbered list
    for(String value : input.getValues()) {
      i++;

      println("  " + i  + " : " + value);

      // Only show last choice if not sensitive
      if(!input.isEmpty() && value.equals(input.getValue()) && !input.isSensitive()) {
        lastChoice = i;
      }
    }

    // Prompt
    reader.printString("Choose: ");

    // Fill previously filled index when available
    if(lastChoice != -1) {
      reader.putString(Integer.toString(lastChoice));
    }

    reader.flushConsole();
    String userTyped;
    if(input.isSensitive()) {
      userTyped = reader.readLine('*');
    } else {
      userTyped = reader.readLine();
    }

    if (userTyped == null) {
      return false;
    } else if (userTyped.isEmpty()) {
      input.setEmpty();
    } else {
      Integer index;
      try {
        index = Integer.valueOf(userTyped);

        if(index < 0 || index >= input.getValues().length) {
          errorMessage("Invalid index");
          return fillInputEnum(input, reader, bundle);
        }

        input.setValue(input.getValues()[index]);
      } catch (NumberFormatException ex) {
        errorMessage("Input is not valid integer number");
        return fillInputEnum(input, reader, bundle);
      }
    }

    return true;
  }

  /**
   * Load user input for map type.
   *
   * This implementation will load one map entry at the time. Current flows is
   * as follows: if user did not enter anything (empty input) finish loading
   * and return from function. If user specified input with equal sign (=),
   * lets add new key value pair. Otherwise consider entire input as a key name
   * and try to remove it from the map.
   *
   * Please note that following code do not supports equal sign in property
   * name. It's however perfectly fine to have equal sign in value.
   *
   * @param input Input that we should read or edit
   * @param reader Associated console reader
   * @param bundle Resource bundle
   * @return True if user wish to continue with loading additional inputs
   * @throws IOException
   */
  private static boolean fillInputMap(MMapInput input,
                                      ConsoleReader reader,
                                      ResourceBundle bundle)
                                      throws IOException {
    // Special prompt in Map case
    println(bundle.getString(input.getLabelKey()) + ": ");

    // Internal loading map
    Map<String, String> values = input.getValue();
    if(values == null) {
      values = new HashMap<String, String>();
    }

    String userTyped;

    while(true) {
      // Print all current items in each iteration
      // However do not printout if this input contains sensitive information.
      println("There are currently " + values.size() + " values in the map:");
      if (!input.isSensitive()) {
        for(Map.Entry<String, String> entry : values.entrySet()) {
          println(entry.getKey() + " = " + entry.getValue());
        }
      }

      // Special prompt for Map entry
      reader.printString("entry# ");
      reader.flushConsole();

      if(input.isSensitive()) {
        userTyped = reader.readLine('*');
      } else {
        userTyped = reader.readLine();
      }

      if(userTyped == null) {
        // Finish loading and return back to Sqoop shell
        return false;
      } else if(userTyped.isEmpty()) {
        // User has finished loading data to Map input, either set input empty
        // if there are no entries or propagate entries to the input
        if(values.size() == 0) {
          input.setEmpty();
        } else {
          input.setValue(values);
        }
        return true;
      } else {
        // User has specified regular input, let's check if it contains equals
        // sign. Save new entry (or update existing one) if it does. Otherwise
        // try to remove entry that user specified.
        if(userTyped.contains("=")) {
          String []keyValue = userTyped.split("=", 2);
          values.put(handleUserInput(keyValue[0]), handleUserInput(keyValue[1]));
        } else {
          String key = handleUserInput(userTyped);
          if(values.containsKey(key)) {
            values.remove(key);
          } else {
            errorMessage("Don't know what to do with " + userTyped);
          }
        }
      }

    }
  }

  /**
   * Handle special cases in user input.
   *
   * Preserve null and empty values, remove whitespace characters before and
   * after loaded string and de-quote the string if it's quoted (to preserve
   * spaces for example).
   *
   * @param input String loaded from user
   * @return Unquoted transformed string
   */
  private static String handleUserInput(String input) {
    // Preserve null and empty values
    if(input == null) {
      return null;
    }
    if(input.isEmpty()) {
      return input;
    }

    // Removes empty characters at the begging and end of loaded string
    input = input.trim();

    int lastIndex = input.length() - 1;
    char first = input.charAt(0);
    char last = input.charAt(lastIndex);

    // Remove quoting if present
    if(first == '\'' && last == '\'') {
      input = input.substring(1, lastIndex);
    } else if(first == '"' && last == '"') {
      input =  input.substring(1, lastIndex);
    }

    // Return final string
    return input;
  }

  private static boolean fillInputInteger(MIntegerInput input,
                                          ConsoleReader reader,
                                          ResourceBundle bundle)
                                          throws IOException {
    generatePrompt(reader, bundle, input);

    // Fill already filled data when available
    // However do not printout if this input contains sensitive information.
    if(!input.isEmpty() && !input.isSensitive()) {
      reader.putString(input.getValue().toString());
    }

    // Get the data
    String userTyped;
    if(input.isSensitive()) {
      userTyped = reader.readLine('*');
    } else {
      userTyped = reader.readLine();
    }

    if (userTyped == null) {
      return false;
    } else if (userTyped.isEmpty()) {
      input.setEmpty();
    } else {
      Integer value;
      try {
        value = Integer.valueOf(userTyped);
        input.setValue(value);
      } catch (NumberFormatException ex) {
        errorMessage("Input is not valid integer number");
        return fillInputInteger(input, reader, bundle);
      }

      input.setValue(Integer.valueOf(userTyped));
    }

    return true;
  }

  /**
   * Load string input from the user.
   *
   * @param input Input that we should load in
   * @param reader Associated console reader
   * @param bundle Resource bundle for this input
   * @return
   * @throws IOException
   */
  public static boolean fillInputString(MStringInput input,
                                        ConsoleReader reader,
                                        ResourceBundle bundle)
                                        throws IOException {
    generatePrompt(reader, bundle, input);

    // Fill already filled data when available
    // However do not printout if this input contains sensitive information.
    if(!input.isEmpty() && !input.isSensitive()) {
      reader.putString(input.getValue());
    }

    // Get the data
    String userTyped;
    if(input.isSensitive()) {
       userTyped = reader.readLine('*');
    } else {
      userTyped = reader.readLine();
    }

    if (userTyped == null) {
      // Propagate end of loading process
      return false;
    } else if (userTyped.isEmpty()) {
      // Empty input in case that nothing was given
      input.setEmpty();
    } else {
      // Set value that user has entered
      input.setValue(userTyped);

      // Check that it did not exceeds maximal allowance for given input
      if(userTyped.length() > input.getMaxLength()) {
        errorMessage("Size of input exceeds allowance for this input"
          + " field. Maximal allowed size is " + input.getMaxLength());
        return fillInputString(input, reader, bundle);
      }
    }

    return true;
  }

  public static void generatePrompt(ConsoleReader reader,
                                    ResourceBundle bundle,
                                    MInput input)
                                    throws IOException {
    reader.printString(bundle.getString(input.getLabelKey()) + ": ");
    reader.flushConsole();
  }

  public static String getName(ConsoleReader reader,
                               String name) throws IOException {
    if(name == null) {
      nameInput.setEmpty();
    } else {
      nameInput.setValue(name);
    }

    fillInputString(nameInput, reader, getResourceBundle());

    return nameInput.getValue();
  }

  /**
   * Print validation message in cases that it's not in state "FINE"
   *
   * @param element Validated element
   */
  public static void printValidationMessage(MValidatedElement element) {
    switch (element.getValidationStatus()) {
      case UNACCEPTABLE:
        errorMessage(element.getValidationMessage());
        break;
      case ACCEPTABLE:
        warningMessage(element.getValidationMessage());
        break;
      default:
        // Simply ignore all other states for the moment
        break;
    }
  }

  public static void errorMessage(String message) {
    println("Error message: @|red " + message + " |@");
  }

  public static void warningMessage(String message) {
    println("Warning message: @|yellow " + message + " |@");
  }

  public static void errorIntroduction() {
    println();
    println("@|red There are issues with entered data, please revise your input:|@");
  }

  private FormFiller() {
    // Do not instantiate
  }
}
