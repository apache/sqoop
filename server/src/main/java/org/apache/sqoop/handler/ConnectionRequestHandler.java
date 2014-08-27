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
package org.apache.sqoop.handler;

import org.apache.log4j.Logger;
import org.apache.sqoop.audit.AuditLoggerManager;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.ConnectorManager;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.framework.FrameworkManager;
import org.apache.sqoop.json.ConnectionBean;
import org.apache.sqoop.json.JsonBean;
import org.apache.sqoop.json.ValidationResultBean;
import org.apache.sqoop.model.FormUtils;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.repository.Repository;
import org.apache.sqoop.repository.RepositoryManager;
import org.apache.sqoop.server.RequestContext;
import org.apache.sqoop.server.RequestHandler;
import org.apache.sqoop.server.common.ServerError;
import org.apache.sqoop.utils.ClassUtils;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.ValidationResult;
import org.apache.sqoop.validation.ValidationRunner;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

/**
 * Connection request handler is supporting following resources:
 *
 * GET /v1/connection/:xid
 * Return details about one particular connection with id :xid or about all of
 * them if :xid equals to "all".
 *
 * POST /v1/connection
 * Create new connection
 *
 * PUT /v1/connection/:xid
 * Update connection with id :xid.
 *
 * PUT /v1/connection/:xid/enable
 * Enable connection with id :xid
 *
 * PUT /v1/connection/:xid/disable
 * Disable connection with id :xid
 *
 * DELETE /v1/connection/:xid
 * Remove connection with id :xid
 *
 * Planned resources:
 *
 * GET /v1/connection
 * Get brief list of all connections present in the system.
 *
 */
public class ConnectionRequestHandler implements RequestHandler {

  private static final Logger LOG =
      Logger.getLogger(ConnectionRequestHandler.class);

  private static final String ENABLE = "enable";
  private static final String DISABLE = "disable";

  public ConnectionRequestHandler() {
    LOG.info("ConnectionRequestHandler initialized");
  }

  @Override
  public JsonBean handleEvent(RequestContext ctx) {
    switch (ctx.getMethod()) {
      case GET:
        return getConnections(ctx);
      case POST:
          return createUpdateConnection(ctx, false);
      case PUT:
        if (ctx.getLastURLElement().equals(ENABLE)) {
          return enableConnection(ctx, true);
        } else if (ctx.getLastURLElement().equals(DISABLE)) {
          return enableConnection(ctx, false);
        } else {
          return createUpdateConnection(ctx, true);
        }
      case DELETE:
        return deleteConnection(ctx);
    }

    return null;
  }

  /**
   * Delete connection from metadata repository.
   *
   * @param ctx Context object
   * @return Empty bean
   */
  private JsonBean deleteConnection(RequestContext ctx) {
    String sxid = ctx.getLastURLElement();
    long xid = Long.valueOf(sxid);

    AuditLoggerManager.getInstance()
        .logAuditEvent(ctx.getUserName(), ctx.getRequest().getRemoteAddr(),
        "delete", "connection", sxid);

    Repository repository = RepositoryManager.getInstance().getRepository();
    repository.deleteConnection(xid);

    return JsonBean.EMPTY_BEAN;
  }

  /**
   * Update or create connection metadata in repository.
   *
   * @param ctx Context object
   * @return Validation bean object
   */
  private JsonBean createUpdateConnection(RequestContext ctx, boolean update) {
//    Check that given ID equals with sent ID, otherwise report an error UPDATE
//    String sxid = ctx.getLastURLElement();
//    long xid = Long.valueOf(sxid);

    String username = ctx.getUserName();

    ConnectionBean bean = new ConnectionBean();

    try {
      JSONObject json =
        (JSONObject) JSONValue.parse(ctx.getRequest().getReader());

      bean.restore(json);
    } catch (IOException e) {
      throw new SqoopException(ServerError.SERVER_0003,
        "Can't read request content", e);
    }

    // Get connection object
    List<MConnection> connections = bean.getConnections();

    if(connections.size() != 1) {
      throw new SqoopException(ServerError.SERVER_0003,
        "Expected one connection metadata but got " + connections.size());
    }

    MConnection connection = connections.get(0);

    // Verify that user is not trying to spoof us
    MConnectionForms connectorForms =
      ConnectorManager.getInstance().getConnectorMetadata(connection.getConnectorId())
      .getConnectionForms();
    MConnectionForms frameworkForms = FrameworkManager.getInstance().getFramework()
      .getConnectionForms();

    if(!connectorForms.equals(connection.getConnectorPart())
      || !frameworkForms.equals(connection.getFrameworkPart())) {
      throw new SqoopException(ServerError.SERVER_0003,
        "Detected incorrect form structure");
    }

    // Responsible connector for this session
    SqoopConnector connector = ConnectorManager.getInstance().getConnector(connection.getConnectorId());

    // We need translate forms to configuration objects
    Object connectorConfig = ClassUtils.instantiate(connector.getConnectionConfigurationClass());
    Object frameworkConfig = ClassUtils.instantiate(FrameworkManager.getInstance().getConnectionConfigurationClass());

    FormUtils.fromForms(connection.getConnectorPart().getForms(), connectorConfig);
    FormUtils.fromForms(connection.getFrameworkPart().getForms(), frameworkConfig);

    // Validate both parts
    ValidationRunner validationRunner = new ValidationRunner();
    ValidationResult connectorValidation = validationRunner.validate(connectorConfig);
    ValidationResult frameworkValidation = validationRunner.validate(frameworkConfig);

    Status finalStatus = Status.getWorstStatus(connectorValidation.getStatus(), frameworkValidation.getStatus());

    // Return back validations in all cases
    ValidationResultBean outputBean = new ValidationResultBean(connectorValidation, frameworkValidation);

    // If we're good enough let's perform the action
    if(finalStatus.canProceed()) {
      if(update) {
        AuditLoggerManager.getInstance()
            .logAuditEvent(ctx.getUserName(), ctx.getRequest().getRemoteAddr(),
            "update", "connection", String.valueOf(connection.getPersistenceId()));

        connection.setLastUpdateUser(username);
        RepositoryManager.getInstance().getRepository().updateConnection(connection);
      } else {
        connection.setCreationUser(username);
        connection.setLastUpdateUser(username);
        RepositoryManager.getInstance().getRepository().createConnection(connection);
        outputBean.setId(connection.getPersistenceId());

        AuditLoggerManager.getInstance()
            .logAuditEvent(ctx.getUserName(), ctx.getRequest().getRemoteAddr(),
            "create", "connection", String.valueOf(connection.getPersistenceId()));
      }
    }

    return outputBean;
  }

  private JsonBean getConnections(RequestContext ctx) {
    String sxid = ctx.getLastURLElement();
    ConnectionBean bean;

    AuditLoggerManager.getInstance()
        .logAuditEvent(ctx.getUserName(), ctx.getRequest().getRemoteAddr(),
        "get", "connection", sxid);

    Locale locale = ctx.getAcceptLanguageHeader();
    Repository repository = RepositoryManager.getInstance().getRepository();

    if (sxid.equals("all")) {

      List<MConnection> connections = repository.findConnections();
      bean = new ConnectionBean(connections);

      // Add associated resources into the bean
      for( MConnection connection : connections) {
        long connectorId = connection.getConnectorId();
        if(!bean.hasConnectorBundle(connectorId)) {
          bean.addConnectorBundle(connectorId,
            ConnectorManager.getInstance().getResourceBundle(connectorId, locale));
        }
      }
    } else {
      long xid = Long.valueOf(sxid);

      MConnection connection = repository.findConnection(xid);
      long connectorId = connection.getConnectorId();

      bean = new ConnectionBean(connection);

      bean.addConnectorBundle(connectorId,
        ConnectorManager.getInstance().getResourceBundle(connectorId, locale));
    }

    // Sent framework resource bundle in all cases
    bean.setFrameworkBundle(FrameworkManager.getInstance().getBundle(locale));

    return bean;
  }

  private JsonBean enableConnection(RequestContext ctx, boolean enabled) {
    String[] elements = ctx.getUrlElements();
    String sxid = elements[elements.length - 2];
    long xid = Long.valueOf(sxid);

    Repository repository = RepositoryManager.getInstance().getRepository();
    repository.enableConnection(xid, enabled);

    return JsonBean.EMPTY_BEAN;
  }
}
