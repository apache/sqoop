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

import org.apache.sqoop.model.MEnumInput;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MFramework;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MInputType;
import org.apache.sqoop.model.MIntegerInput;
import org.apache.sqoop.model.MJobForms;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.utils.StringUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

import static org.apache.sqoop.client.shell.ShellEnvironment.*;

/**
 * Convenience static methods for displaying form related information
 */
public final class FormDisplayer {

  public static void displayFormMetadataDetails(MFramework framework,
                                                ResourceBundle bundle) {
    print("  Supported job types: ");
    println(framework.getAllJobsForms().keySet().toString());

    displayFormsMetadata(
      framework.getConnectionForms().getForms(),
      "Connection",
      bundle);

    for (MJobForms jobForms : framework.getAllJobsForms().values()) {
      print("  Forms for job type ");
      print(jobForms.getType().name());
      println(":");

      displayFormsMetadata(jobForms.getForms(), "Job", bundle);
    }
  }

  public static void displayFormsMetadata(List<MForm> forms,
                                         String type,
                                         ResourceBundle bundle) {
    Iterator<MForm> fiter = forms.iterator();
    int findx = 1;
    while (fiter.hasNext()) {
      print("    ");
      print(type);
      print(" form ");
      print(findx++);
      println(":");

      MForm form = fiter.next();
      print("      Name: ");
      println(form.getName());

      // Label
      print("      Label: ");
      println(bundle.getString(form.getLabelKey()));

      // Help text
      print("      Help: ");
      println(bundle.getString(form.getHelpKey()));

      List<MInput<?>> inputs = form.getInputs();
      Iterator<MInput<?>> iiter = inputs.iterator();
      int iindx = 1;
      while (iiter.hasNext()) {
        print("      Input ");
        print(iindx++);
        println(":");

        MInput<?> input = iiter.next();
        print("        Name: ");
        println(input.getName());
        print("        Label: ");
        println(bundle.getString(input.getLabelKey()));
        print("        Help: ");
        println(bundle.getString(input.getHelpKey()));
        print("        Type: ");
        println(input.getType());
        if (input.getType() == MInputType.STRING) {
          print("        Mask: ");
          println(((MStringInput)input).isMasked());
          print("        Size: ");
          println(((MStringInput)input).getMaxLength());
        } else if(input.getType() == MInputType.ENUM) {
          print("        Possible values: ");
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

  private static void displayForm(MForm form, ResourceBundle bundle) {
    print("  ");
    println(bundle.getString(form.getLabelKey()));

    for (MInput<?> input : form.getInputs()) {
      print("    ");
      print(bundle.getString(input.getLabelKey()));
      print(": ");
      if(!input.isEmpty()) {
        // Based on the input type, let's perform specific load
        switch (input.getType()) {
          case STRING:
            displayInputString((MStringInput) input);
            break;
          case INTEGER:
            displayInputInteger((MIntegerInput) input);
            break;
          case MAP:
            displayInputMap((MMapInput) input);
            break;
          case ENUM:
            displayInputEnum((MEnumInput) input);
            break;
          default:
            println("Unsupported data type " + input.getType());
            return;
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
    if (input.isMasked()) {
      print("(This input is sensitive)");
    } else {
      print(input.getValue());
    }
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
