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

import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.model.MAccountableEntity;
import org.apache.sqoop.model.MBooleanInput;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MEnumInput;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MFramework;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MInputType;
import org.apache.sqoop.model.MIntegerInput;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.validation.Message;
import org.apache.sqoop.validation.Status;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

import static org.apache.sqoop.shell.ShellEnvironment.*;

/**
 * Convenience static methods for displaying form related information
 */
public final class FormDisplayer {

  public static void displayFormMetadataDetails(MFramework framework,
                                                ResourceBundle bundle) {
    displayFormsMetadata(
      framework.getConnectionForms().getForms(),
      resourceString(Constants.RES_FORMDISPLAYER_CONNECTION),
      bundle);

    displayFormsMetadata(
      framework.getJobForms().getForms(),
      resourceString(Constants.RES_FORMDISPLAYER_JOB),
      bundle);
  }

  public static void displayFormMetadataDetails(MConnector connector,
                                                ResourceBundle bundle) {
    displayFormsMetadata(
        connector.getConnectionForms().getForms(),
        resourceString(Constants.RES_FORMDISPLAYER_CONNECTION),
        bundle);

    // @TODO(Abe): Validate From/To output is correct.
    displayFormsMetadata(
        connector.getJobForms(Direction.FROM).getForms(),
        Direction.FROM.toString() + " " + resourceString(Constants.RES_FORMDISPLAYER_JOB),
        bundle);

    displayFormsMetadata(
        connector.getJobForms(Direction.TO).getForms(),
        Direction.TO.toString() + " " + resourceString(Constants.RES_FORMDISPLAYER_JOB),
        bundle);
  }

  public static void displayFormsMetadata(List<MForm> forms,
                                         String type,
                                         ResourceBundle bundle) {
    Iterator<MForm> fiter = forms.iterator();
    int findx = 1;
    while (fiter.hasNext()) {
      print("    ");
      print(type);
      print(" %s ", resourceString(Constants.RES_FORMDISPLAYER_FORM));
      print(findx++);
      println(":");

      MForm form = fiter.next();
      print("      %s: ", resourceString(Constants.RES_FORMDISPLAYER_NAME));
      println(form.getName());

      // Label
      print("      %s: ", resourceString(Constants.RES_FORMDISPLAYER_LABEL));
      println(bundle.getString(form.getLabelKey()));

      // Help text
      print("      %s: ", resourceString(Constants.RES_FORMDISPLAYER_HELP));
      println(bundle.getString(form.getHelpKey()));

      List<MInput<?>> inputs = form.getInputs();
      Iterator<MInput<?>> iiter = inputs.iterator();
      int iindx = 1;
      while (iiter.hasNext()) {
        print("      %s ", resourceString(Constants.RES_FORMDISPLAYER_INPUT));
        print(iindx++);
        println(":");

        MInput<?> input = iiter.next();
        print("        %s: ", resourceString(Constants.RES_FORMDISPLAYER_NAME));
        println(input.getName());
        print("        %s: ", resourceString(Constants.RES_FORMDISPLAYER_LABEL));
        println(bundle.getString(input.getLabelKey()));
        print("        %s: ", resourceString(Constants.RES_FORMDISPLAYER_HELP));
        println(bundle.getString(input.getHelpKey()));
        print("        %s: ", resourceString(Constants.RES_FORMDISPLAYER_TYPE));
        println(input.getType());
        print("        %s: ", resourceString(Constants.RES_FORMDISPLAYER_SENSITIVE));
        println(input.isSensitive());
        if (input.getType() == MInputType.STRING) {
          print("        %s: ", resourceString(Constants.RES_FORMDISPLAYER_SIZE));
          println(((MStringInput)input).getMaxLength());
        } else if(input.getType() == MInputType.ENUM) {
          print("        %s: ", resourceString(Constants.RES_FORMDISPLAYER_POSSIBLE_VALUES));
          println(StringUtils.join(((MEnumInput)input).getValues(), ","));
        }
      }
    }
  }

  public static void displayForms(List<MForm> forms, ResourceBundle bundle) {
    for(MForm form : forms) {
      displayForm(form, bundle);
    }
  }

  /**
   * Method prints the warning message of ACCEPTABLE status
   * @param entity - connection or job instance
   */
  public static void displayFormWarning(MAccountableEntity entity) {
    List<MForm> formList = new ArrayList<MForm>();
    boolean showMessage = true;
    if (entity instanceof MConnection) {
      MConnection connection = (MConnection) entity;
      formList.addAll(connection.getConnectorPart().getForms());
      formList.addAll(connection.getFrameworkPart().getForms());
    } else if(entity instanceof MJob) {
      MJob job = (MJob) entity;
      formList.addAll(job.getConnectorPart(Direction.FROM).getForms());
      formList.addAll(job.getFrameworkPart().getForms());
      formList.addAll(job.getConnectorPart(Direction.TO).getForms());
    }
    for(MForm form : formList) {
      if(form.getValidationStatus() == Status.ACCEPTABLE) {
        if(showMessage) {
          print("\n@|yellow %s|@\n", resourceString(Constants.RES_FORMDISPLAYER_FORM_WARNING));
          showMessage = false;
        }
        for(Message message : form.getValidationMessages()) {
          FormFiller.warningMessage(message.getMessage());
        }
      }
    }
  }

  private static void displayForm(MForm form, ResourceBundle bundle) {
    print("  ");
    println(bundle.getString(form.getLabelKey()));

    for (MInput<?> input : form.getInputs()) {
      print("    ");
      print(bundle.getString(input.getLabelKey()));
      print(": ");
      if(!input.isEmpty()) {
        if (input.isSensitive()) {
          print("(%s)", resourceString(Constants.RES_FORMDISPLAYER_INPUT_SENSITIVE));
        } else {
          // Based on the input type, let's perform specific load
          switch (input.getType()) {
            case STRING:
              displayInputString((MStringInput) input);
              break;
            case INTEGER:
              displayInputInteger((MIntegerInput) input);
              break;
            case BOOLEAN:
              displayInputBoolean((MBooleanInput) input);
              break;
            case MAP:
              displayInputMap((MMapInput) input);
              break;
            case ENUM:
              displayInputEnum((MEnumInput) input);
              break;
            default:
              print("\n%s " + input.getType(), resourceString(Constants.RES_FORMDISPLAYER_UNSUPPORTED_DATATYPE));
              return;
          }
        }
      }
      println("");
    }
  }

  /**
   * Display content of String input.
   *
   * @param input String input
   */
  private static void displayInputString(MStringInput input) {
    print(input.getValue());
  }

  /**
   * Display content of Integer input.
   *
   * @param input Integer input
   */
  private static void displayInputInteger(MIntegerInput input) {
    print(input.getValue());
  }

  /**
   * Display content of Boolean input.
   *
   * @param input Boolean input
   */
  private static void displayInputBoolean(MBooleanInput input) {
    print(input.getValue());
  }

  /**
   * Display content of Map input
   *
   * @param input Map input
   */
  private static void displayInputMap(MMapInput input) {
    for(Map.Entry<String, String> entry : input.getValue().entrySet()) {
      println();
      print("      ");
      print(entry.getKey());
      print(" = ");
      print(entry.getValue());
    }
  }

  /**
   * Display content of Enum input
   *
   * @param input Enum input
   */
  private static void displayInputEnum(MEnumInput input) {
    print(input.getValue());
  }

  private FormDisplayer() {
    // Do not instantiate
  }
}
