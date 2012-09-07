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

import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MFramework;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MInputType;
import org.apache.sqoop.model.MJobForms;
import org.apache.sqoop.model.MStringInput;
import org.codehaus.groovy.tools.shell.IO;

import java.util.Iterator;
import java.util.List;
import java.util.ResourceBundle;

/**
 * Convenience static methods for displaying form related information
 */
public class FormDisplayer {

  public static void displayFormMetadataDetails(IO io,
                                                MFramework framework,
                                                ResourceBundle bundle) {
    io.out.print("  Supported job types: ");
    io.out.println(framework.getAllJobsForms().keySet().toString());

    displayFormsMetadata(io,
      framework.getConnectionForms().getForms(),
      "Connection",
      bundle);

    for (MJobForms jobForms : framework.getAllJobsForms().values()) {
      io.out.print("  Forms for job type ");
      io.out.print(jobForms.getType().name());
      io.out.println(":");

      displayFormsMetadata(io, jobForms.getForms(), "Job", bundle);
    }
  }

  public static void displayFormsMetadata(IO io,
                                         List<MForm> forms,
                                         String type,
                                         ResourceBundle bundle) {
    Iterator<MForm> fiter = forms.iterator();
    int findx = 1;
    while (fiter.hasNext()) {
      io.out.print("    ");
      io.out.print(type);
      io.out.print(" form ");
      io.out.print(findx++);
      io.out.println(":");

      MForm form = fiter.next();
      io.out.print("      Name: ");
      io.out.println(form.getName());

      // Label
      io.out.print("      Label: ");
      io.out.println(bundle.getString(form.getLabelKey()));

      // Help text
      io.out.print("      Help: ");
      io.out.println(bundle.getString(form.getHelpKey()));

      List<MInput<?>> inputs = form.getInputs();
      Iterator<MInput<?>> iiter = inputs.iterator();
      int iindx = 1;
      while (iiter.hasNext()) {
        io.out.print("      Input ");
        io.out.print(iindx++);
        io.out.println(":");

        MInput<?> input = iiter.next();
        io.out.print("        Name: ");
        io.out.println(input.getName());
        io.out.print("        Label: ");
        io.out.println(bundle.getString(input.getLabelKey()));
        io.out.print("        Help: ");
        io.out.println(bundle.getString(input.getHelpKey()));
        io.out.print("        Type: ");
        io.out.println(input.getType());
        if (input.getType() == MInputType.STRING) {
          io.out.print("        Mask: ");
          io.out.println(((MStringInput)input).isMasked());
          io.out.print("        Size: ");
          io.out.println(((MStringInput)input).getMaxLength());
        }
      }
    }
  }

  public static void displayForms(IO io,
                                  List<MForm> forms,
                                  ResourceBundle bundle) {
    for(MForm form : forms) {
      displayForm(io, form, bundle);
    }
  }

  private static void displayForm(IO io, MForm form, ResourceBundle bundle) {
    io.out.print("  ");
    io.out.println(bundle.getString(form.getLabelKey()));

    for (MInput<?> input : form.getInputs()) {
      io.out.print("    ");
      io.out.print(bundle.getString(input.getLabelKey()));
      io.out.print(": ");
      if(!input.isEmpty()) {
        io.out.print(input.getUrlSafeValueString());
      }
      io.out.println("");
    }
  }

  private FormDisplayer() {
    // Do not instantiate
  }
}
