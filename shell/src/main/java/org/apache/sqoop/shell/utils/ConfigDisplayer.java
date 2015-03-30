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

import static org.apache.sqoop.shell.ShellEnvironment.print;
import static org.apache.sqoop.shell.ShellEnvironment.println;
import static org.apache.sqoop.shell.ShellEnvironment.resourceString;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.model.MAccountableEntity;
import org.apache.sqoop.model.MBooleanInput;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MEnumInput;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MInputType;
import org.apache.sqoop.model.MIntegerInput;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLongInput;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.validation.Message;
import org.apache.sqoop.validation.Status;

/**
 * Convenience static methods for displaying config related information
 */
public final class ConfigDisplayer {

  public static void displayDriverConfigDetails(MDriverConfig driverConfig, ResourceBundle bundle) {
    displayConfig(driverConfig.getConfigs(),
        resourceString(Constants.RES_CONFIG_DISPLAYER_JOB), bundle);
  }

  public static void displayConnectorConfigDetails(MConnector connector, ResourceBundle bundle) {
    displayConfig(
        connector.getLinkConfig().getConfigs(),
        resourceString(Constants.RES_CONFIG_DISPLAYER_LINK),
        bundle);

    for (Direction direction : new Direction[]{Direction.FROM, Direction.TO}) {
      if (connector.getSupportedDirections().isDirectionSupported(direction)) {
        List<MConfig> configs = direction.equals(Direction.FROM)
                ? connector.getFromConfig().getConfigs()
                : connector.getToConfig().getConfigs();
        displayConfig(
                configs,
                direction.toString() + " " + resourceString(Constants.RES_CONFIG_DISPLAYER_JOB),
                bundle);
      }
    }
  }

   private static void displayConfig(List<MConfig> configs,
                                         String type,
                                         ResourceBundle bundle) {
    Iterator<MConfig> iterator = configs.iterator();
    int findx = 1;
    while (iterator.hasNext()) {
      print("    ");
      print(type);
      print(" %s ", resourceString(Constants.RES_CONFIG_DISPLAYER_CONFIG));
      print(findx++);
      println(":");

      MConfig config = iterator.next();
      print("      %s: ", resourceString(Constants.RES_CONFIG_DISPLAYER_NAME));
      println(config.getName());

      // Label
      print("      %s: ", resourceString(Constants.RES_CONFIG_DISPLAYER_LABEL));
      println(bundle.getString(config.getLabelKey()));

      // Help text
      print("      %s: ", resourceString(Constants.RES_CONFIG_DISPLAYER_HELP));
      println(bundle.getString(config.getHelpKey()));

      List<MInput<?>> inputs = config.getInputs();
      Iterator<MInput<?>> iiter = inputs.iterator();
      int iindx = 1;
      while (iiter.hasNext()) {
        print("      %s ", resourceString(Constants.RES_CONFIG_DISPLAYER_INPUT));
        print(iindx++);
        println(":");

        MInput<?> input = iiter.next();
        print("        %s: ", resourceString(Constants.RES_CONFIG_DISPLAYER_NAME));
        println(input.getName());
        print("        %s: ", resourceString(Constants.RES_CONFIG_DISPLAYER_LABEL));
        println(bundle.getString(input.getLabelKey()));
        print("        %s: ", resourceString(Constants.RES_CONFIG_DISPLAYER_HELP));
        println(bundle.getString(input.getHelpKey()));
        print("        %s: ", resourceString(Constants.RES_CONFIG_DISPLAYER_TYPE));
        println(input.getType());
        print("        %s: ", resourceString(Constants.RES_CONFIG_DISPLAYER_SENSITIVE));
        println(input.isSensitive());
        print("        %s: ", resourceString(Constants.RES_CONFIG_DISPLAYER_EDITABLE));
        println(input.getEditable());
        print("        %s: ", resourceString(Constants.RES_CONFIG_DISPLAYER_OVERRIDES));
        println(input.getOverrides());

        if (input.getType() == MInputType.STRING) {
          print("        %s: ", resourceString(Constants.RES_CONFIG_DISPLAYER_SIZE));
          println(((MStringInput)input).getMaxLength());
        } else if(input.getType() == MInputType.ENUM) {
          print("        %s: ", resourceString(Constants.RES_CONFIG_DISPLAYER_POSSIBLE_VALUES));
          println(StringUtils.join(((MEnumInput)input).getValues(), ","));
        }
      }
    }
  }

  public static void displayConfig(List<MConfig> configs, ResourceBundle bundle) {
    for(MConfig config : configs) {
      displayConfig(config, bundle);
    }
  }

  /**
   * Method prints the warning message of WARNING status
   * @param entity - link or job instance
   */
  public static void displayConfigWarning(MAccountableEntity entity) {
    List<MConfig> configList = new ArrayList<MConfig>();
    boolean showMessage = true;
    if (entity instanceof MLink) {
      MLink link = (MLink) entity;
      configList.addAll(link.getConnectorLinkConfig().getConfigs());
    } else if(entity instanceof MJob) {
      MJob job = (MJob) entity;
      configList.addAll(job.getFromJobConfig().getConfigs());
      configList.addAll(job.getDriverConfig().getConfigs());
      configList.addAll(job.getToJobConfig().getConfigs());
    }
    for(MConfig config : configList) {
      if(config.getValidationStatus() == Status.WARNING) {
        if(showMessage) {
          print("\n@|yellow %s|@\n", resourceString(Constants.RES_CONFIG_DISPLAYER_FORM_WARNING));
          showMessage = false;
        }
        for(Message message : config.getValidationMessages()) {
          ConfigFiller.warningMessage(message.getMessage());
        }
      }
    }
  }

  private static void displayConfig(MConfig config, ResourceBundle bundle) {
    print("  ");
    println(bundle.getString(config.getLabelKey()));

    for (MInput<?> input : config.getInputs()) {
      print("    ");
      print(bundle.getString(input.getLabelKey()));
      print(": ");
      if(!input.isEmpty()) {
        if (input.isSensitive()) {
          print("(%s)", resourceString(Constants.RES_CONFIG_DISPLAYER_INPUT_SENSITIVE));
        } else {
          // Based on the input type, let's perconfig specific load
          switch (input.getType()) {
            case STRING:
              displayInputString((MStringInput) input);
              break;
            case INTEGER:
              displayInputInteger((MIntegerInput) input);
              break;
            case LONG:
              displayLongInteger((MLongInput) input);
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
              print("\n%s " + input.getType(), resourceString(Constants.RES_CONFIG_DISPLAYER_UNSUPPORTED_DATATYPE));
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
   * Display content of Long input.
   *
   * @param input Long input
   */
  private static void displayLongInteger(MLongInput input) {
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

  private ConfigDisplayer() {
    // Do not instantiate
  }
}
