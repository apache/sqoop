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
package org.apache.sqoop.shell.utils;

import jline.ConsoleReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.model.MBooleanInput;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MEnumInput;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MIntegerInput;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MNamedElement;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.model.MValidatedElement;
import org.apache.sqoop.validation.Message;
import org.apache.sqoop.validation.Status;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ResourceBundle;

import static org.apache.sqoop.shell.ShellEnvironment.*;

/**
 * Convenient methods for retrieving user input and CLI options.
 */
public final class FormFiller {

  /**
   * Internal input that will be reused for loading names for connection and
   * job objects.
   */
  private static MStringInput nameInput = new MStringInput("object-name", false, (short)25);

  /**
   * Fill job object based on CLI options.
   *
   * @param line Associated console reader object
   * @param job Job that user is suppose to fill in
   * @return True if we filled all inputs, false if user has stopped processing
   * @throws IOException
   */
  public static boolean fillJob(CommandLine line,
                                MJob job)
                                throws IOException {

    job.setName(line.getOptionValue("name"));

    // Fill in data from user
    return fillForms(line,
                     job.getConnectorPart(Direction.FROM).getForms(),
                     job.getFrameworkPart().getForms());
  }

  /**
   * Fill job object based on user input.
   *
   * @param reader Associated console reader object
   * @param job Job that user is suppose to fill in
   * @param fromConnectorBundle Connector resource bundle
   * @param frameworkBundle Framework resource bundle
   * @return True if we filled all inputs, false if user has stopped processing
   * @throws IOException
   */
  public static boolean fillJob(ConsoleReader reader,
                                MJob job,
                                ResourceBundle fromConnectorBundle,
                                ResourceBundle frameworkBundle,
                                ResourceBundle toConnectorBundle)
                                throws IOException {

    job.setName(getName(reader, job.getName()));

    // Fill in data from user
    return fillForms(reader,
                     job.getConnectorPart(Direction.FROM).getForms(),
                     fromConnectorBundle,
                     job.getFrameworkPart().getForms(),
                     frameworkBundle,
                     job.getConnectorPart(Direction.TO).getForms(),
                     toConnectorBundle);
  }

  /**
   * Fill connection object based on CLI options.
   *
   * @param line Associated command line options
   * @param connection Connection that user is suppose to fill in
   * @return True if we filled all inputs, false if user has stopped processing
   * @throws IOException
   */
  public static boolean fillConnection(CommandLine line,
                                       MConnection connection)
                                       throws IOException {

    connection.setName(line.getOptionValue("name"));

    // Fill in data from user
    return fillForms(line,
                     connection.getConnectorPart().getForms(),
                     connection.getFrameworkPart().getForms());
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

  /**
   * Load CLI options for framework forms and connector forms.
   *
   * @param line CLI options container
   * @param connectorForms Connector forms to read or edit
   * @param frameworkForms Framework forms to read or edit
   * @return
   * @throws IOException
   */
  public static boolean fillForms(CommandLine line,
                                  List<MForm> connectorForms,
                                  List<MForm> frameworkForms)
                                      throws IOException {
    // Query connector forms and framework forms
    return fillForms("connector", connectorForms, line)
        && fillForms("framework", frameworkForms, line);
  }

  /**
   * Load all CLI options for a list of forms.
   *
   * @param prefix placed at the beginning of the CLI option key
   * @param forms Forms to read or edit
   * @param line CLI options container
   * @return
   * @throws IOException
   */
  public static boolean fillForms(String prefix,
                                  List<MForm> forms,
                                  CommandLine line)
                                  throws IOException {
    for (MForm form : forms) {
      if (!fillForm(prefix, form, line)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Load all CLI options for a particular form.
   *
   * @param prefix placed at the beginning of the CLI option key
   * @param form Form to read or edit
   * @param line CLI options container
   * @return
   * @throws IOException
   */
  @SuppressWarnings("rawtypes")
  public static boolean fillForm(String prefix,
                                 MForm form,
                                 CommandLine line) throws IOException {
    for (MInput input : form.getInputs()) {
      if (!fillInput(prefix, input, line)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Load CLI option.
   * Chooses the appropriate 'fill' method to use based on input type.
   *
   * Keys for CLI options are automatically created from the 'prefix' argument
   * and 'input' argument: <prefix>-<form name>-<input name>
   *
   * @param prefix placed at the beginning of the CLI option key
   * @param input Input that we should read or edit
   * @param line CLI options container
   * @return
   * @throws IOException
   */
  @SuppressWarnings("rawtypes")
  public static boolean fillInput(String prefix,
                                  MInput input,
                                  CommandLine line) throws IOException {
    // Based on the input type, let's perform specific load
    switch (input.getType()) {
    case STRING:
      return fillInputString(prefix, (MStringInput) input, line);
    case INTEGER:
      return fillInputInteger(prefix, (MIntegerInput) input, line);
    case BOOLEAN:
      return fillInputBoolean(prefix, (MBooleanInput) input, line);
    case MAP:
      return fillInputMap(prefix, (MMapInput) input, line);
    case ENUM:
      return fillInputEnum(prefix, (MEnumInput) input, line);
    default:
      println("Unsupported data type " + input.getType());
      return true;
    }
  }

  /**
   * Load CLI option for enum type.
   *
   * Currently only supports numeric values.
   *
   * @param prefix placed at the beginning of the CLI option key
   * @param input Input that we should read or edit
   * @param line CLI options container
   * @return
   * @throws IOException
   */
  private static boolean fillInputEnum(String prefix,
                                       MEnumInput input,
                                       CommandLine line)
                                       throws IOException {
    String opt = FormOptions.getOptionKey(prefix, input);
    if (line.hasOption(opt)) {
      String value = line.getOptionValue(opt);
      int index = java.util.Arrays.asList(input.getValues()).indexOf(value);

      if(index < 0) {
        errorMessage(input, String.format("Invalid option %s. Please use one of %s.", value, StringUtils.join(input.getValues(), ", ")));
        return false;
      }

      input.setValue(value);
    } else {
      input.setEmpty();
    }
    return true;
  }

  /**
   * Load CLI options for map type.
   *
   * Parses Key-Value pairs that take the form "<key>=<value>&<key>=<value>&...".
   *
   * @param prefix placed at the beginning of the CLI option key
   * @param input Input that we should read or edit
   * @param line CLI options container
   * @return
   * @throws IOException
   */
  private static boolean fillInputMap(String prefix,
                                      MMapInput input,
                                      CommandLine line)
                                      throws IOException {
    String opt = FormOptions.getOptionKey(prefix, input);
    if (line.hasOption(opt)) {
      String value = line.getOptionValue(opt);
      Map<String, String> values = new HashMap<String, String>();
      String[] entries = value.split("&");
      for (String entry : entries) {
        if (entry.contains("=")) {
          String[] keyValue = entry.split("=");
          values.put(keyValue[0], keyValue[1]);
        } else {
          errorMessage(input, "Don't know what to do with " + entry);
          return false;
        }
      }
      input.setValue(values);
    } else {
      input.setEmpty();
    }
    return true;
  }

  /**
   * Load integer input from CLI option.
   *
   * @param prefix placed at the beginning of the CLI option key
   * @param input Input that we should read or edit
   * @param line CLI options container
   * @return
   * @throws IOException
   */
  private static boolean fillInputInteger(String prefix,
                                          MIntegerInput input,
                                          CommandLine line)
                                          throws IOException {
    String opt = FormOptions.getOptionKey(prefix, input);
    if (line.hasOption(opt)) {
      try {
        input.setValue(Integer.valueOf(line.getOptionValue(FormOptions.getOptionKey(prefix, input))));
      } catch (NumberFormatException ex) {
        errorMessage(input, "Input is not valid integer number");
        return false;
      }
    } else {
      input.setEmpty();
    }
    return true;
  }

  /**
   * Load string input from CLI option.
   *
   * @param prefix placed at the beginning of the CLI option key
   * @param input Input that we should read or edit
   * @param line CLI options container
   * @return
   * @throws IOException
   */
  public static boolean fillInputString(String prefix,
                                        MStringInput input,
                                        CommandLine line)
                                        throws IOException {
    String opt = FormOptions.getOptionKey(prefix, input);
    if (line.hasOption(opt)) {
      String value = line.getOptionValue(FormOptions.getOptionKey(prefix, input));
      if(value.length() > input.getMaxLength()) {
        errorMessage(input, "Size of input exceeds allowance for this input"
          + " field. Maximal allowed size is " + input.getMaxLength());
      }
      input.setValue(value);
    } else {
      input.setEmpty();
    }
    return true;
  }

  /**
   * Load boolean input from CLI option.
   *
   * @param prefix placed at the beginning of the CLI option key
   * @param input Input that we should read or edit
   * @param line CLI options container
   * @return
   * @throws IOException
   */
  public static boolean fillInputBoolean(String prefix,
                                         MBooleanInput input,
                                         CommandLine line)
                                         throws IOException {
    String opt = FormOptions.getOptionKey(prefix, input);
    if (line.hasOption(opt)) {
      input.setValue(Boolean.valueOf(line.getOptionValue(FormOptions.getOptionKey(prefix, input))));
    } else {
      input.setEmpty();
    }
    return true;
  }

  public static boolean fillForms(ConsoleReader reader,
                                  List<MForm> connectorForms,
                                  ResourceBundle connectorBundle,
                                  List<MForm> frameworkForms,
                                  ResourceBundle frameworkBundle) throws IOException {


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

  public static boolean fillForms(ConsoleReader reader,
                                  List<MForm> fromConnectorForms,
                                  ResourceBundle fromConnectorBundle,
                                  List<MForm> frameworkForms,
                                  ResourceBundle frameworkBundle,
                                  List<MForm> toConnectorForms,
                                  ResourceBundle toConnectorBundle) throws IOException {


    // From connector forms
    if(!fillForms(fromConnectorForms, reader, fromConnectorBundle)) {
      return false;
    }

    // Query framework forms
    if(!fillForms(frameworkForms, reader, frameworkBundle)) {
      return false;
    }

    // To connector forms
    if(!fillForms(toConnectorForms, reader, toConnectorBundle)) {
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

  @SuppressWarnings("rawtypes")
  public static boolean fillForm(MForm form,
                                 ConsoleReader reader,
                                 ResourceBundle bundle) throws IOException {
    println("");
    println(bundle.getString(form.getLabelKey()));

    // Print out form validation
    printValidationMessage(form, false);
    println("");

    for (MInput input : form.getInputs()) {
      if(!fillInput(input, reader, bundle)) {
        return false;
      }
    }

    return true;
  }

  @SuppressWarnings("rawtypes")
  public static boolean fillInput(MInput input,
                                  ConsoleReader reader,
                                  ResourceBundle bundle) throws IOException {
    // Print out validation
    printValidationMessage(input, false);

    // Based on the input type, let's perform specific load
    switch (input.getType()) {
      case STRING:
        return fillInputString((MStringInput) input, reader, bundle);
      case INTEGER:
        return fillInputInteger((MIntegerInput) input, reader, bundle);
      case BOOLEAN:
        return fillInputBoolean((MBooleanInput) input, reader, bundle);
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

  /**
   * Load boolean input from the user.
   *
   * @param input Input that we should load in
   * @param reader Associated console reader
   * @param bundle Resource bundle for this input
   * @return
   * @throws IOException
   */
  public static boolean fillInputBoolean(MBooleanInput input,
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
      // Propagate end of loading process
      return false;
    } else if (userTyped.isEmpty()) {
      // Empty input in case that nothing was given
      input.setEmpty();
    } else {
      // Set value that user has entered
      input.setValue(Boolean.valueOf(userTyped));
    }

    return true;
  }

  @SuppressWarnings("rawtypes")
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
  public static void printValidationMessage(MValidatedElement element, boolean includeInputPrefix) {
    if(element.getValidationStatus() == Status.getDefault()) {
      return;
    }

    for(Message message : element.getValidationMessages())
    switch (message.getStatus()) {
      case UNACCEPTABLE:
        if (includeInputPrefix) {
          errorMessage(element, message.getMessage());
        } else {
          errorMessage(message.getMessage());
        }
        break;
      case ACCEPTABLE:
        if (includeInputPrefix) {
          warningMessage(element, message.getMessage());
        } else {
          warningMessage(message.getMessage());
        }
        break;
      default:
        // Simply ignore all other states for the moment
        break;
    }
  }

  public static void errorMessage(String message) {
    println("Error message: @|red " + message + " |@");
  }

  public static void errorMessage(MNamedElement input, String message) {
    print(input.getName());
    print(": ");
    errorMessage(message);
  }

  public static void warningMessage(String message) {
    println("Warning message: @|yellow " + message + " |@");
  }

  public static void warningMessage(MNamedElement input, String message) {
    print(input.getName());
    print(": ");
    warningMessage(message);
  }

  public static void errorIntroduction() {
    println();
    println("@|red There are issues with entered data, please revise your input:|@");
  }

  public static void printConnectionValidationMessages(MConnection connection) {
    for (MForm form : connection.getConnectorPart().getForms()) {
      for (MInput<?> input : form.getInputs()) {
        printValidationMessage(input, true);
      }
    }
    for (MForm form : connection.getFrameworkPart().getForms()) {
      for (MInput<?> input : form.getInputs()) {
        printValidationMessage(input, true);
      }
    }
  }

  public static void printJobValidationMessages(MJob job) {
    for (MForm form : job.getConnectorPart(Direction.FROM).getForms()) {
      for (MInput<?> input : form.getInputs()) {
        printValidationMessage(input, true);
      }
    }
    for (MForm form : job.getFrameworkPart().getForms()) {
      for (MInput<?> input : form.getInputs()) {
        printValidationMessage(input, true);
      }
    }
    for (MForm form : job.getConnectorPart(Direction.TO).getForms()) {
      for (MInput<?> input : form.getInputs()) {
        printValidationMessage(input, true);
      }
    }
  }

  private FormFiller() {
    // Do not instantiate
  }
}
