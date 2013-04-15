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
package org.apache.sqoop.client.core;

/**
 *
 */
public class Constants {

  // General string constants
  public static final String RESOURCE_NAME = "client-resource";
  public static final String BOLD_STR_SEQUENCE = "@|bold";
  public static final String END_STR_SEQUENCE = "|@";

  // Environmental variables
  public static final String ENV_HOST = "SQOOP2_HOST";
  public static final String ENV_PORT = "SQOOP2_PORT";
  public static final String ENV_WEBAPP = "SQOOP2_WEBAPP";

  // Options

  public static final String OPT_XID = "xid";
  public static final String OPT_ALL = "all";
  public static final String OPT_JID = "jid";
  public static final String OPT_CID = "cid";
  public static final String OPT_TYPE = "type";
  public static final String OPT_NAME = "name";
  public static final String OPT_VALUE = "value";
  public static final String OPT_VERBOSE = "verbose";
  public static final String OPT_HOST = "host";
  public static final String OPT_PORT = "port";
  public static final String OPT_WEBAPP = "webapp";
  public static final String OPT_SERVER = "server";
  public static final String OPT_CLIENT = "client";
  public static final String OPT_PROTOCOL = "protocol";
  public static final String OPT_SYNCHRONOUS = "synchronous";
  public static final String OPT_POLL_TIMEOUT = "poll-timeout";

  public static final char OPT_XID_CHAR = 'x';
  public static final char OPT_ALL_CHAR = 'a';
  public static final char OPT_JID_CHAR = 'j';
  public static final char OPT_CID_CHAR = 'c';
  public static final char OPT_TYPE_CHAR = 't';
  public static final char OPT_NAME_CHAR = 'n';
  public static final char OPT_VALUE_CHAR = 'v';
  public static final char OPT_HOST_CHAR = 'h';
  public static final char OPT_PORT_CHAR = 'p';
  public static final char OPT_WEBAPP_CHAR = 'w';
  public static final char OPT_SERVER_CHAR = 's';
  public static final char OPT_CLIENT_CHAR = 'c';
  public static final char OPT_PROTOCOL_CHAR = 'p';

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

  public static final String CMD_SUBMISSION = "submission";
  public static final String CMD_SUBMISSION_SC = "\\sub";

  public static final String CMD_UPDATE = "update";
  public static final String CMD_UPDATE_SC = "\\up";

  public static final String FN_CONNECTION = "connection";
  public static final String FN_JOB = "job";
  public static final String FN_SERVER = "server";
  public static final String FN_OPTION = "option";
  public static final String FN_CONNECTOR = "connector";
  public static final String FN_VERSION = "version";
  public static final String FN_FRAMEWORK = "framework";
  public static final String FN_START = "start";
  public static final String FN_STOP = "stop";
  public static final String FN_STATUS = "status";

  public static final String PRE_CLONE = "Clone";
  public static final String PRE_CREATE = "Create";
  public static final String PRE_DELETE = "Delete";
  public static final String PRE_SET = "Set";
  public static final String PRE_SHOW = "Show";
  public static final String PRE_SUBMISSION = "Submission";
  public static final String PRE_UPDATE = "Update";
  public static final String SUF_INFO = "Info";


  public static final String PROP_HOMEDIR = "user.home";
  public static final String PROP_CURDIR = "user.dir";
  public static final String SQOOP_PROMPT = "sqoop";


  // Resource Keys for various messages

  public static final String RES_FUNCTION_UNKNOWN =
      "args.function.unknown";
  public static final String RES_ARGS_XID_MISSING =
      "args.xid_missing";
  public static final String RES_ARGS_JID_MISSING =
      "args.jid_missing";
  public static final String RES_ARGS_CID_MISSING =
      "args.cid_missing";
  public static final String RES_ARGS_TYPE_MISSING =
      "args.type_missing";
  public static final String RES_ARGS_NAME_MISSING =
      "args.name_missing";
  public static final String RES_ARGS_VALUE_MISSING =
      "args.value_missing";

  public static final String RES_PROMPT_CONN_ID =
      "prompt.conn_id";
  public static final String RES_PROMPT_JOB_ID =
      "prompt.job_id";
  public static final String RES_CONNECTOR_ID =
      "prompt.connector_id";
  public static final String RES_PROMPT_JOB_TYPE =
      "prompt.job_type";
  public static final String RES_PROMPT_UPDATE_CONN_METADATA =
      "prompt.update_conn_metadata";
  public static final String RES_PROMPT_UPDATE_JOB_METADATA =
      "prompt.update_job_metadata";
  public static final String RES_PROMPT_FILL_CONN_METADATA =
      "prompt.fill_conn_metadata";
  public static final String RES_PROMPT_FILL_JOB_METADATA =
      "prompt.fill_job_metadata";

  public static final String RES_CLONE_USAGE =
      "clone.usage";
  public static final String RES_CLONE_CONN_SUCCESSFUL =
      "clone.conn.successful";
  public static final String RES_CLONE_JOB_SUCCESSFUL =
      "clone.job.successful";
  public static final String RES_CLONE_CLONING_CONN =
      "clone.cloning_conn";
  public static final String RES_CLONE_CLONING_JOB =
      "clone.cloning_job";

  public static final String RES_CREATE_USAGE =
      "create.usage";
  public static final String RES_CREATE_CONN_SUCCESSFUL =
      "create.conn_successful";
  public static final String RES_CREATE_JOB_SUCCESSFUL =
      "create.job_successful";
  public static final String RES_CREATE_CREATING_CONN =
      "create.creating_conn";
  public static final String RES_CREATE_CREATING_JOB =
      "create.creating_job";

  public static final String RES_DELETE_USAGE =
      "delete.usage";

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

  public static final String RES_SET_USAGE =
      "set.usage";
  public static final String RES_SET_PROMPT_OPT_NAME =
      "set.prompt_opt_name";
  public static final String RES_SET_PROMPT_OPT_VALUE =
      "set.prompt_opt_value";
  public static final String RES_SET_VERBOSE_CHANGED =
      "set.verbose_changed";
  public static final String RES_SET_UNKNOWN_OPT_IGNORED =
      "set.unknown_opt_ignored";
  public static final String RES_SET_HOST_DESCRIPTION =
      "set.host_description";
  public static final String RES_SET_PORT_DESCRIPTION =
      "set.port_description";
  public static final String RES_WEBAPP_DESCRIPTION =
      "set.webapp_description";
  public static final String RES_SET_SERVER_USAGE =
      "set.server_usage";
  public static final String RES_SET_SERVER_SUCCESSFUL =
      "set.server_successful";

  public static final String RES_SHOW_USAGE =
      "show.usage";
  public static final String RES_SHOW_PROMPT_DISPLAY_ALL_CONNS =
      "show.prompt_display_all_conns";
  public static final String RES_SHOW_PROMPT_DISPLAY_CONN_XID =
      "show.prompt_display_conn_xid";
  public static final String RES_SHOW_CONN_USAGE =
      "show.conn_usage";
  public static final String RES_SHOW_PROMPT_CONNS_TO_SHOW =
      "show.prompt_conns_to_show";
  public static final String RES_SHOW_PROMPT_CONN_INFO =
      "show.prompt_conn_info";
  public static final String RES_SHOW_PROMPT_CONN_CID_INFO =
      "show.prompt_conn_cid_info";

  public static final String RES_SHOW_PROMPT_DISPLAY_ALL_CONNECTORS =
      "show.prompt_display_all_connectors";
  public static final String RES_SHOW_PROMPT_DISPLAY_CONNECTOR_CID =
      "show.prompt_display_connector_cid";
  public static final String RES_SHOW_CONNECTOR_USAGE =
      "show.connector_usage";
  public static final String RES_SHOW_PROMPT_CONNECTORS_TO_SHOW =
      "show.prompt_connectors_to_show";
  public static final String RES_SHOW_PROMPT_CONNECTOR_INFO =
      "show.prompt_connector_info";

  public static final String RES_SHOW_FRAMEWORK_USAGE =
      "show.framework_usage";
  public static final String RES_SHOW_PROMPT_FRAMEWORK_OPTS =
      "show.prompt_framework_opts";

  public static final String RES_SHOW_PROMPT_DISPLAY_ALL_JOBS =
      "show.prompt_display_all_jobs";
  public static final String RES_SHOW_PROMPT_DISPLAY_JOB_JID =
      "show.prompt_display_job_jid";
  public static final String RES_SHOW_JOB_USAGE =
      "show.job_usage";
  public static final String RES_SHOW_PROMPT_JOBS_TO_SHOW =
      "show.prompt_jobs_to_show";
  public static final String RES_SHOW_PROMPT_JOB_INFO =
      "show.prompt_job_info";
  public static final String RES_SHOW_PROMPT_JOB_XID_CID_INFO =
      "show.prompt_job_xid_cid_info";

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
  public static final String RES_SHOW_PROMPT_DISPLAY_VERSION_SERVER =
      "show.prompt_display_version_server";
  public static final String RES_SHOW_PROMPT_DISPLAY_VERSION_CLIENT =
      "show.prompt_display_version_client";
  public static final String RES_SHOW_PROMPT_DISPLAY_VERSION_PROTOCOL =
      "show.prompt_display_version_protocol";
  public static final String RES_SHOW_VERSION_USAGE =
      "show.version_usage";
  public static final String RES_SHOW_PROMPT_VERSION_CLIENT_SERVER =
      "show.prompt_version_client_server";
  public static final String RES_SHOW_PROMPT_VERSION_PROTOCOL =
      "show.prompt_version_protocol";

  public static final String RES_SQOOP_SHELL_BANNER =
      "sqoop.shell_banner";
  public static final String RES_SQOOP_PROMPT_SHELL_LOADRC =
      "sqoop.prompt_shell_loadrc";
  public static final String RES_SQOOP_PROMPT_SHELL_LOADEDRC =
      "sqoop.prompt_shell_loadedrc";

  public static final String RES_SUBMISSION_USAGE =
      "submission.usage";
  public static final String RES_PROMPT_SYNCHRONOUS =
      "submission.prompt_synchronous";
  public static final String RES_PROMPT_POLL_TIMEOUT =
      "submission.prompt_poll_timeout";

  public static final String RES_UPDATE_USAGE =
      "update.usage";
  public static final String RES_UPDATE_UPDATING_CONN =
      "update.conn";
  public static final String RES_UPDATE_CONN_SUCCESSFUL =
      "update.conn_successful";
  public static final String RES_UPDATE_UPDATING_JOB =
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
  public static final String RES_TABLE_HEADER_TYPE =
      "table.header.type";
  public static final String RES_TABLE_HEADER_CONNECTOR =
      "table.header.connector";

  public static final String RES_FORMDISPLAYER_SUPPORTED_JOBTYPE =
      "formdisplayer.supported_job_types";
  public static final String RES_FORMDISPLAYER_CONNECTION =
      "formdisplayer.connection";
  public static final String RES_FORMDISPLAYER_JOB =
      "formdisplayer.job";
  public static final String RES_FORMDISPLAYER_FORM_JOBTYPE =
      "formdisplayer.forms_jobtype";
  public static final String RES_FORMDISPLAYER_FORM =
      "formdisplayer.form";
  public static final String RES_FORMDISPLAYER_NAME =
      "formdisplayer.name";
  public static final String RES_FORMDISPLAYER_LABEL =
      "formdisplayer.label";
  public static final String RES_FORMDISPLAYER_HELP =
      "formdisplayer.help";
  public static final String RES_FORMDISPLAYER_INPUT =
      "formdisplayer.input";
  public static final String RES_FORMDISPLAYER_TYPE =
      "formdisplayer.type";
  public static final String RES_FORMDISPLAYER_SENSITIVE =
      "formdisplayer.sensitive";
  public static final String RES_FORMDISPLAYER_SIZE =
      "formdisplayer.size";
  public static final String RES_FORMDISPLAYER_POSSIBLE_VALUES =
      "formdisplayer.possible_values";
  public static final String RES_FORMDISPLAYER_UNSUPPORTED_DATATYPE =
      "formdisplayer.unsupported_datatype";
  public static final String RES_FORMDISPLAYER_INPUT_SENSITIVE =
      "formdisplayer.input_sensitive";

  public static final String RES_FORMDISPLAYER_FORM_WARNING =
      "formdisplayer.warning_message";

  private Constants() {
    // Instantiation is prohibited
  }
}
