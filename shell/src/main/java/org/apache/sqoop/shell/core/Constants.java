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
package org.apache.sqoop.shell.core;

/**
 *
 */
public class Constants {

  // General string constants
  public static final String RESOURCE_NAME = "shell-resource";
  public static final String BOLD_STR_SEQUENCE = "@|bold";
  public static final String END_STR_SEQUENCE = "|@";

  // Environmental variables
  public static final String ENV_HOST = "SQOOP2_HOST";
  public static final String ENV_PORT = "SQOOP2_PORT";
  public static final String ENV_WEBAPP = "SQOOP2_WEBAPP";

  // Options

  public static final String OPT_LID = "lid";
  public static final String OPT_FROM = "from";
  public static final String OPT_TO = "to";
  public static final String OPT_ALL = "all";
  public static final String OPT_JID = "jid";
  public static final String OPT_CID = "cid";
  public static final String OPT_NAME = "name";
  public static final String OPT_VALUE = "value";
  public static final String OPT_VERBOSE = "verbose";
  public static final String OPT_HOST = "host";
  public static final String OPT_PORT = "port";
  public static final String OPT_WEBAPP = "webapp";
  public static final String OPT_URL = "url";
  public static final String OPT_SERVER = "server";
  public static final String OPT_CLIENT = "client";
  public static final String OPT_REST_API = "api";
  public static final String OPT_SYNCHRONOUS = "synchronous";
  public static final String OPT_POLL_TIMEOUT = "poll-timeout";
  public static final String OPT_DETAIL = "detail";
  public static final String OPT_ROLE = "role";
  public static final String OPT_ACTION = "action";
  public static final String OPT_RESOURCE = "resource";
  public static final String OPT_RESOURCE_TYPE = "resource-type";
  public static final String OPT_PRINCIPAL = "principal";
  public static final String OPT_PRINCIPAL_TYPE = "principal-type";
  public static final String OPT_WITH_GRANT = "with-grant";

  public static final char OPT_LID_CHAR = 'l';
  public static final char OPT_FROM_CHAR = 'f';
  public static final char OPT_TO_CHAR = 't';
  public static final char OPT_ALL_CHAR = 'a';
  public static final char OPT_JID_CHAR = 'j';
  public static final char OPT_CID_CHAR = 'c';
  public static final char OPT_NAME_CHAR = 'n';
  public static final char OPT_VALUE_CHAR = 'v';
  public static final char OPT_HOST_CHAR = 'h';
  public static final char OPT_PORT_CHAR = 'p';
  public static final char OPT_WEBAPP_CHAR = 'w';
  public static final char OPT_URL_CHAR = 'u';
  public static final char OPT_SERVER_CHAR = 's';
  public static final char OPT_CLIENT_CHAR = 'c';
  public static final char OPT_API_CHAR = 'p';
  public static final char OPT_SYNCHRONOUS_CHAR = 's';
  public static final char OPT_POLL_TIMEOUT_CHAR = 'p';
  public static final char OPT_DETAIL_CHAR = 'd';
  public static final char OPT_ROLE_CHAR = 'r';
  public static final char OPT_ACTION_CHAR = 'a';
  public static final char OPT_WITH_GRANT_CHAR = 'g';

  // Resource keys for various commands, command options,
  // functions and descriptions
  public static final String CMD_CLONE = "clone";
  public static final String CMD_CLONE_SC = "\\cl";

  public static final String CMD_CREATE = "create";
  public static final String CMD_CREATE_SC = "\\cr";

  public static final String CMD_DELETE = "delete";
  public static final String CMD_DELETE_SC = "\\d";

  public static final String CMD_HELP = "help";
  public static final String CMD_HELP_SC = "\\h";

  public static final String CMD_SET = "set";
  public static final String CMD_SET_SC = "\\st";

  public static final String CMD_SHOW = "show";
  public static final String CMD_SHOW_SC = "\\sh";

  public static final String CMD_UPDATE = "update";
  public static final String CMD_UPDATE_SC = "\\up";

  public static final String CMD_START = "start";
  public static final String CMD_START_SC = "\\sta";

  public static final String CMD_STOP = "stop";
  public static final String CMD_STOP_SC = "\\stp";

  public static final String CMD_STATUS = "status";
  public static final String CMD_STATUS_SC = "\\stu";

  public static final String CMD_ENABLE = "enable";
  public static final String CMD_ENABLE_SC = "\\en";

  public static final String CMD_DISABLE = "disable";
  public static final String CMD_DISABLE_SC = "\\di";

  public static final String CMD_GRANT = "grant";
  public static final String CMD_GRANT_SC = "\\g";

  public static final String CMD_REVOKE = "revoke";
  public static final String CMD_REVOKE_SC = "\\r";

  public static final String FN_LINK = "link";
  public static final String FN_JOB = "job";
  public static final String FN_SUBMISSION = "submission";
  public static final String FN_SERVER = "server";
  public static final String FN_OPTION = "option";
  public static final String FN_CONNECTOR = "connector";
  public static final String FN_VERSION = "version";
  public static final String FN_DRIVER_CONFIG = "driver";
  public static final String FN_ROLE = "role";
  public static final String FN_PRINCIPAL = "principal";
  public static final String FN_PRIVILEGE = "privilege";

  public static final String PROP_HOMEDIR = "user.home";
  public static final String PROP_CURDIR = "user.dir";
  public static final String SQOOP_PROMPT = "sqoop";

  // Shared resources
  public static final String RES_SHARED_USAGE = "shared.usage";
  public static final String RES_SHARED_UNKNOWN_FUNCTION =  "shared.unknown.function";

  // Resource Keys for various messages

  public static final String RES_ARGS_LID_MISSING =
      "args.lid_missing";
  public static final String RES_ARGS_FROM_MISSING =
      "args.from_missing";
  public static final String RES_ARGS_TO_MISSING =
      "args.to_missing";
  public static final String RES_ARGS_JID_MISSING =
      "args.jid_missing";
  public static final String RES_ARGS_CID_MISSING =
      "args.cid_missing";
  public static final String RES_ARGS_NAME_MISSING =
      "args.name_missing";
  public static final String RES_ARGS_VALUE_MISSING =
      "args.value_missing";

  public static final String RES_PROMPT_LINK_ID =
      "prompt.link_id";
  public static final String RES_PROMPT_JOB_ID =
      "prompt.job_id";
  public static final String RES_CONNECTOR_ID =
      "prompt.connector_id";
  public static final String RES_PROMPT_UPDATE_LINK_CONFIG =
      "prompt.update_link_config";
  public static final String RES_PROMPT_UPDATE_JOB_CONFIG =
      "prompt.update_job_config";
  public static final String RES_PROMPT_FILL_LINK_CONFIG =
      "prompt.fill_link_config";
  public static final String RES_PROMPT_FILL_JOB_CONFIG =
      "prompt.fill_job_config";

  public static final String RES_CLONE_LINK_SUCCESSFUL =
      "clone.link.successful";
  public static final String RES_CLONE_JOB_SUCCESSFUL =
      "clone.job.successful";
  public static final String RES_CLONE_CLONING_LINK =
      "clone.cloning_link";
  public static final String RES_CLONE_CLONING_JOB =
      "clone.cloning_job";

  public static final String RES_CREATE_LINK_SUCCESSFUL =
      "create.link_successful";
  public static final String RES_CREATE_JOB_SUCCESSFUL =
      "create.job_successful";
  public static final String RES_CREATE_ROLE_SUCCESSFUL =
      "create.role_successful";
  public static final String RES_CREATE_CREATING_LINK =
      "create.creating_link";
  public static final String RES_CREATE_CREATING_JOB =
      "create.creating_job";

  public static final String RES_DELETE_ROLE_SUCCESSFUL =
      "delete.role_successful";

  public static final String RES_DISABLE_LINK_SUCCESSFUL =
      "disable.link_successful";
  public static final String RES_DISABLE_JOB_SUCCESSFUL =
      "disable.job_successful";

  public static final String RES_ENABLE_LINK_SUCCESSFUL =
      "enable.link_successful";
  public static final String RES_ENABLE_JOB_SUCCESSFUL =
      "enable.job_successful";

  public static final String RES_HELP_USAGE =
      "help.usage";
  public static final String RES_HELP_DESCRIPTION =
      "help.description";
  public static final String RES_HELP_CMD_USAGE =
      "help.cmd_usage";
  public static final String RES_HELP_MESSAGE =
      "help.message";
  public static final String RES_HELP_INFO =
      "help.info";
  public static final String RES_HELP_AVAIL_COMMANDS =
      "help.avail_commands";
  public static final String RES_HELP_CMD_DESCRIPTION =
      "help.cmd_description";
  public static final String RES_HELP_SPECIFIC_CMD_INFO =
      "help.specific_cmd_info";

  public static final String RES_UNRECOGNIZED_CMD =
      "unrecognized.cmd";

  public static final String RES_SET_PROMPT_OPT_NAME =
      "set.prompt_opt_name";
  public static final String RES_SET_PROMPT_OPT_VALUE =
      "set.prompt_opt_value";
  public static final String RES_SET_VERBOSE_CHANGED =
      "set.verbose_changed";
  public static final String RES_SET_POLL_TIMEOUT_CHANGED =
      "set.poll_timeout_changed";
  public static final String RES_SET_UNKNOWN_OPT_IGNORED =
      "set.unknown_opt_ignored";
  public static final String RES_SET_HOST_DESCRIPTION =
      "set.host_description";
  public static final String RES_SET_PORT_DESCRIPTION =
      "set.port_description";
  public static final String RES_WEBAPP_DESCRIPTION =
      "set.webapp_description";
  public static final String RES_URL_DESCRIPTION =
      "set.url_description";
  public static final String RES_SET_SERVER_USAGE =
      "set.server_usage";
  public static final String RES_SET_SERVER_SUCCESSFUL =
      "set.server_successful";
  public static final String RES_SET_SERVER_IGNORED =
      "set.server_ignored";

  public static final String RES_SHOW_PROMPT_DISPLAY_ALL_LINKS =
      "show.prompt_display_all_links";
  public static final String RES_SHOW_PROMPT_DISPLAY_LINK_LID =
      "show.prompt_display_link_lid";
  public static final String RES_SHOW_PROMPT_LINKS_TO_SHOW =
      "show.prompt_links_to_show";
  public static final String RES_SHOW_PROMPT_LINK_INFO =
      "show.prompt_link_info";
  public static final String RES_SHOW_PROMPT_LINK_CID_INFO =
      "show.prompt_link_cid_info";
  public static final String RES_SHOW_ROLE_BAD_ARGUMENTS_PRINCIPAL_TYPE =
      "show.role.bad_arguments_principal_type";
  public static final String RES_SHOW_PRIVILEGE_BAD_ARGUMENTS_RESOURCE_TYPE =
      "show.privilege.bad_arguments_resource_type";

  public static final String RES_SHOW_PROMPT_DISPLAY_ALL_CONNECTORS =
      "show.prompt_display_all_connectors";
  public static final String RES_SHOW_PROMPT_DISPLAY_CONNECTOR_CID =
      "show.prompt_display_connector_cid";
  public static final String RES_SHOW_PROMPT_CONNECTORS_TO_SHOW =
      "show.prompt_connectors_to_show";
  public static final String RES_SHOW_PROMPT_CONNECTOR_INFO =
      "show.prompt_connector_info";

  public static final String RES_SHOW_DRIVER_USAGE =
      "show.driver_usage";
  public static final String RES_SHOW_PROMPT_DRIVER_OPTS =
      "show.prompt_driver_opts";

  public static final String RES_SHOW_PROMPT_DISPLAY_ALL_JOBS =
      "show.prompt_display_all_jobs";
  public static final String RES_SHOW_PROMPT_DISPLAY_JOBS_CID =
      "show.prompt_display_all_jobs_cid";
  public static final String RES_SHOW_PROMPT_DISPLAY_JOB_JID =
      "show.prompt_display_job_jid";
  public static final String RES_SHOW_PROMPT_JOBS_TO_SHOW =
      "show.prompt_jobs_to_show";
  public static final String RES_SHOW_PROMPT_JOB_INFO =
      "show.prompt_job_info";
  public static final String RES_SHOW_PROMPT_JOB_FROM_LID_INFO =
      "show.prompt_job_from_lid_info";
  public static final String RES_SHOW_PROMPT_JOB_TO_LID_INFO =
      "show.prompt_job_to_lid_info";

  public static final String RES_SHOW_PROMPT_DISPLAY_ALL_SUBMISSIONS =
      "show.prompt_display_all_submissions";
  public static final String RES_SHOW_PROMPT_DISPLAY_ALL_SUBMISSIONS_JOB_ID =
      "show.prompt_display_all_submissions_jid";

  public static final String RES_SHOW_PROMPT_DISPLAY_ALL_SERVERS =
      "show.prompt_display_all_servers";
  public static final String RES_SHOW_PROMPT_DISPLAY_SERVER_HOST =
      "show.prompt_display_server_host";
  public static final String RES_SHOW_PROMPT_DISPLAY_SERVER_PORT =
      "show.prompt_display_server_port";
  public static final String RES_SHOW_PROMPT_DISPLAY_SERVER_WEBAPP =
      "show.prompt_display_server_webapp";
  public static final String RES_SHOW_SERVER_USAGE =
      "show.server_usage";
  public static final String RES_SHOW_PROMPT_SERVER_HOST =
      "show.prompt_server_host";
  public static final String RES_SHOW_PROMPT_SERVER_PORT =
      "show.prompt_server_port";
  public static final String RES_SHOW_PROMPT_SERVER_WEBAPP =
      "show.prompt_server_webapp";

  public static final String RES_SHOW_PROMPT_DISPLAY_ALL_VERSIONS =
      "show.prompt_display_all_versions";
  public static final String RES_SHOW_PROMPT_DISPLAY_SERVER_VERSION =
      "show.prompt_display_version_server";
  public static final String RES_SHOW_PROMPT_DISPLAY_CLIENT_VERSION =
      "show.prompt_display_version_client";
  public static final String RES_SHOW_PROMPT_DISPLAY_REST_API_VERSION =
      "show.prompt_display_version_api";
  public static final String RES_SHOW_VERSION_USAGE =
      "show.version_usage";
  public static final String RES_SHOW_PROMPT_VERSION_CLIENT_SERVER =
      "show.prompt_version_client_server";
  public static final String RES_SHOW_PROMPT_API_VERSIONS =
      "show.prompt_version_api";

  public static final String RES_PROMPT_SYNCHRONOUS =
      "start.prompt_synchronous";

  public static final String RES_SQOOP_SHELL_BANNER =
      "sqoop.shell_banner";
  public static final String RES_SQOOP_PROMPT_SHELL_LOADRC =
      "sqoop.prompt_shell_loadrc";
  public static final String RES_SQOOP_PROMPT_SHELL_LOADEDRC =
      "sqoop.prompt_shell_loadedrc";

  public static final String RES_SQOOP_UPDATING_LINK =
      "update.link";
  public static final String RES_UPDATE_LINK_SUCCESSFUL =
      "update.link_successful";
  public static final String RES_SQOOP_UPDATING_JOB =
      "update.job";
  public static final String RES_UPDATE_JOB_SUCCESSFUL =
      "update.job_successful";

  public static final String RES_TABLE_HEADER_ID =
      "table.header.id";
  public static final String RES_TABLE_HEADER_NAME =
      "table.header.name";
  public static final String RES_TABLE_HEADER_VERSION =
      "table.header.version";
  public static final String RES_TABLE_HEADER_CLASS =
      "table.header.class";
  public static final String RES_TABLE_HEADER_SUPPORTED_DIRECTIONS =
      "table.header.supported_directions";
  public static final String RES_TABLE_HEADER_CONNECTOR_NAME =
      "table.header.connector.name";
  public static final String RES_TABLE_HEADER_CONNECTOR_ID =
      "table.header.connector.id";
  public static final String RES_TABLE_HEADER_FROM_CONNECTOR =
      "table.header.connector.from";
  public static final String RES_TABLE_HEADER_TO_CONNECTOR =
      "table.header.connector.to";
  public static final String RES_TABLE_HEADER_JOB_ID =
      "table.header.jid";
  public static final String RES_TABLE_HEADER_EXTERNAL_ID =
      "table.header.eid";
  public static final String RES_TABLE_HEADER_STATUS =
      "table.header.status";
  public static final String RES_TABLE_HEADER_DATE =
      "table.header.date";
  public static final String RES_TABLE_HEADER_ENABLED =
      "table.header.enabled";
  public static final String RES_TABLE_HEADER_ROLE_NAME =
      "table.header.role.name";
  public static final String RES_TABLE_HEADER_RESOURCE_NAME =
      "table.header.resource.name";
  public static final String RES_TABLE_HEADER_RESOURCE_TYPE =
      "table.header.resource.type";
  public static final String RES_TABLE_HEADER_PRIVILEGE_ACTION =
      "table.header.privilege.action";
  public static final String RES_TABLE_HEADER_PRIVILEGE_WITH_GRANT =
      "table.header.privilege.with_grant";
  public static final String RES_TABLE_HEADER_PRINCIPAL_NAME =
      "table.header.principal.name";
  public static final String RES_TABLE_HEADER_PRINCIPAL_TYPE =
      "table.header.principal.type";

  public static final String RES_CONFIG_DISPLAYER_LINK =
      "config.displayer.link";
  public static final String RES_CONFIG_DISPLAYER_JOB =
      "config.displayer.job";
  public static final String RES_CONFIG_DISPLAYER_CONFIG =
      "config.displayer.config";
  public static final String RES_CONFIG_DISPLAYER_NAME =
      "config.displayer.name";
  public static final String RES_CONFIG_DISPLAYER_LABEL =
      "config.displayer.label";
  public static final String RES_CONFIG_DISPLAYER_HELP =
      "config.displayer.help";
  public static final String RES_CONFIG_DISPLAYER_INPUT =
      "config.displayer.input";
  public static final String RES_CONFIG_DISPLAYER_TYPE =
      "config.displayer.type";
  public static final String RES_CONFIG_DISPLAYER_SENSITIVE =
      "config.displayer.sensitive";
  public static final String RES_CONFIG_DISPLAYER_EDITABLE =
      "config.displayer.editable";
  public static final String RES_CONFIG_DISPLAYER_OVERRIDES =
      "config.displayer.overrides";
  public static final String RES_CONFIG_DISPLAYER_SIZE =
      "config.displayer.size";
  public static final String RES_CONFIG_DISPLAYER_POSSIBLE_VALUES =
      "config.displayer.possible_values";
  public static final String RES_CONFIG_DISPLAYER_UNSUPPORTED_DATATYPE =
      "config.displayer.unsupported_datatype";
  public static final String RES_CONFIG_DISPLAYER_INPUT_SENSITIVE =
      "config.displayer.input_sensitive";

  public static final String RES_CONFIG_DISPLAYER_FORM_WARNING =
      "config.displayer.warning_message";

  public static final String RES_SUBMISSION_SUBMISSION_DETAIL =
      "submission.submission_detail";
  public static final String RES_SUBMISSION_JOB_ID =
      "submission.job_id";
  public static final String RES_SUBMISSION_CREATION_USER =
      "submission.creation_user";
  public static final String RES_SUBMISSION_CREATION_DATE =
      "submission.creation_date";
  public static final String RES_SUBMISSION_UPDATE_USER =
      "submission.update_user";
  public static final String RES_SUBMISSION_EXTERNAL_ID =
      "submission.external_id";
  public static final String RES_SUBMISSION_PROGRESS_NOT_AVAIL =
      "submission.progress_not_available";
  public static final String RES_SUBMISSION_COUNTERS =
      "submission.counters";
  public static final String RES_SUBMISSION_EXECUTED_SUCCESS =
      "submission.executed_success";
  public static final String RES_SUBMISSION_SERVER_URL =
      "submission.server_url";
  public static final String RES_FROM_SCHEMA =
      "submission.from_schema";
  public static final String RES_TO_SCHEMA =
    "submission.to_schema";

  public static final String RES_GRANT_ROLE_SUCCESSFUL =
    "grant.role_successful";
  public static final String RES_GRANT_PRIVILEGE_SUCCESSFUL =
    "grant.privilege_successful";
  public static final String RES_GRANT_PRIVILEGE_SUCCESSFUL_WITH_GRANT =
    "grant.privilege_successful_with_grant";
  public static final String RES_REVOKE_ROLE_SUCCESSFUL =
    "revoke.role_successful";
  public static final String RES_REVOKE_PRIVILEGE_SUCCESSFUL =
    "revoke.privilege_successful";
  public static final String RES_REVOKE_PRIVILEGE_SUCCESSFUL_WITH_GRANT =
    "revoke.privilege_successful_with_grant";

  public static final String RES_PROMPT_ROLE =
    "prompt.role";
  public static final String RES_PROMPT_RESOURCE_TYPE =
    "prompt.resource_type";
  public static final String RES_PROMPT_RESOURCE =
    "prompt.resource";
  public static final String RES_PROMPT_ACTION =
    "prompt.action";
  public static final String RES_PROMPT_PRINCIPAL =
    "prompt.principal";
  public static final String RES_PROMPT_PRINCIPAL_TYPE =
    "prompt.principal_type";
  public static final String RES_PROMPT_WITH_GRANT =
    "prompt.with_grant";

  private Constants() {
    // Instantiation is prohibited
  }
}
