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

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.apache.log4j.Logger;
import org.apache.sqoop.audit.AuditLoggerManager;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.ConnectorManager;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.json.JsonBean;
import org.apache.sqoop.json.LinkBean;
import org.apache.sqoop.json.ValidationResultBean;
import org.apache.sqoop.model.ConfigUtils;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.repository.Repository;
import org.apache.sqoop.repository.RepositoryManager;
import org.apache.sqoop.server.RequestContext;
import org.apache.sqoop.server.RequestHandler;
import org.apache.sqoop.server.common.ServerError;
import org.apache.sqoop.utils.ClassUtils;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.ConfigValidationResult;
import org.apache.sqoop.validation.ConfigValidationRunner;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * Connection request handler is supporting following resources:
 *
 * GET /v1/link/:xid
 * Return details about one particular link with id :xid or about all of
 * them if :xid equals to "all".
 *
 * POST /v1/link
 * Create new link
 *
 * PUT /v1/link/:xid
 * Update link with id :xid.
 *
 * PUT /v1/link/:xid/enable
 * Enable link with id :xid
 *
 * PUT /v1/link/:xid/disable
 * Disable link with id :xid
 *
 * DELETE /v1/link/:xid
 * Remove link with id :xid
 *
 * Planned resources:
 *
 * GET /v1/link
 * Get brief list of all links present in the system.
 *
 */
public class LinkRequestHandler implements RequestHandler {

  private static final Logger LOG =
      Logger.getLogger(LinkRequestHandler.class);

  private static final String ENABLE = "enable";
  private static final String DISABLE = "disable";

  public LinkRequestHandler() {
    LOG.info("LinkRequestHandler initialized");
  }

  @Override
  public JsonBean handleEvent(RequestContext ctx) {
    switch (ctx.getMethod()) {
      case GET:
        return getLink(ctx);
      case POST:
          return createUpdateLink(ctx, false);
      case PUT:
        if (ctx.getLastURLElement().equals(ENABLE)) {
          return enableLink(ctx, true);
        } else if (ctx.getLastURLElement().equals(DISABLE)) {
          return enableLink(ctx, false);
        } else {
          return createUpdateLink(ctx, true);
        }
      case DELETE:
        return deleteLink(ctx);
    }

    return null;
  }

  /**
   * Delete link from thes repository.
   *
   * @param ctx Context object
   * @return Empty bean
   */
  private JsonBean deleteLink(RequestContext ctx) {
    String sxid = ctx.getLastURLElement();
    long xid = Long.valueOf(sxid);

    AuditLoggerManager.getInstance()
        .logAuditEvent(ctx.getUserName(), ctx.getRequest().getRemoteAddr(),
        "delete", "link", sxid);

    Repository repository = RepositoryManager.getInstance().getRepository();
    repository.deleteLink(xid);

    return JsonBean.EMPTY_BEAN;
  }

  /**
   * Update or create link in repository.
   *
   * @param ctx Context object
   * @return Validation bean object
   */
  private JsonBean createUpdateLink(RequestContext ctx, boolean update) {

    String username = ctx.getUserName();
    LinkBean bean = new LinkBean();
    try {
      JSONObject json =
        (JSONObject) JSONValue.parse(ctx.getRequest().getReader());

      bean.restore(json);
    } catch (IOException e) {
      throw new SqoopException(ServerError.SERVER_0003,
        "Can't read request content", e);
    }

    // Get link object
    List<MLink> links = bean.getLinks();

    if(links.size() != 1) {
      throw new SqoopException(ServerError.SERVER_0003,
        "Expected one link but got " + links.size());
    }

    MLink link = links.get(0);

    // Verify that user is not trying to spoof us
    MLinkConfig linkConfig =
      ConnectorManager.getInstance().getConnectorConfigurable(link.getConnectorId())
      .getLinkConfig();
    if(!linkConfig.equals(link.getConnectorLinkConfig())) {
      throw new SqoopException(ServerError.SERVER_0003,
        "Detected incorrect config structure");
    }

    // Responsible connector for this session
    SqoopConnector connector = ConnectorManager.getInstance().getSqoopConnector(link.getConnectorId());

    // We need translate configs
    Object connectorLinkConfig = ClassUtils.instantiate(connector.getLinkConfigurationClass());

    ConfigUtils.fromConfigs(link.getConnectorLinkConfig().getConfigs(), connectorLinkConfig);

    // Validate both parts
    ConfigValidationRunner validationRunner = new ConfigValidationRunner();
    ConfigValidationResult connectorLinkValidation = validationRunner.validate(connectorLinkConfig);

    Status finalStatus = Status.getWorstStatus(connectorLinkValidation.getStatus());

    // Return back validations in all cases
    ValidationResultBean outputBean = new ValidationResultBean(connectorLinkValidation);

    // If we're good enough let's perform the action
    if(finalStatus.canProceed()) {
      if(update) {
        AuditLoggerManager.getInstance()
            .logAuditEvent(ctx.getUserName(), ctx.getRequest().getRemoteAddr(),
            "update", "link", String.valueOf(link.getPersistenceId()));

        link.setLastUpdateUser(username);
        RepositoryManager.getInstance().getRepository().updateLink(link);
      } else {
        link.setCreationUser(username);
        link.setLastUpdateUser(username);
        RepositoryManager.getInstance().getRepository().createLink(link);
        outputBean.setId(link.getPersistenceId());

        AuditLoggerManager.getInstance()
            .logAuditEvent(ctx.getUserName(), ctx.getRequest().getRemoteAddr(),
            "create", "link", String.valueOf(link.getPersistenceId()));
      }
    }

    return outputBean;
  }

  private JsonBean getLink(RequestContext ctx) {
    String sxid = ctx.getLastURLElement();
    LinkBean bean;

    AuditLoggerManager.getInstance()
        .logAuditEvent(ctx.getUserName(), ctx.getRequest().getRemoteAddr(),
        "get", "link", sxid);

    Locale locale = ctx.getAcceptLanguageHeader();
    Repository repository = RepositoryManager.getInstance().getRepository();

    if (sxid.equals("all")) {

      List<MLink> links = repository.findLinks();
      bean = new LinkBean(links);

      // Add associated resources into the bean
      for( MLink link : links) {
        long connectorId = link.getConnectorId();
        if(!bean.hasConnectorConfigBundle(connectorId)) {
          bean.addConnectorConfigBundle(connectorId,
            ConnectorManager.getInstance().getResourceBundle(connectorId, locale));
        }
      }
    } else {
      long xid = Long.valueOf(sxid);

      MLink link = repository.findLink(xid);
      long connectorId = link.getConnectorId();

      bean = new LinkBean(link);

      bean.addConnectorConfigBundle(connectorId,
        ConnectorManager.getInstance().getResourceBundle(connectorId, locale));
    }
    return bean;
  }

  private JsonBean enableLink(RequestContext ctx, boolean enabled) {
    String[] elements = ctx.getUrlElements();
    String sLinkId = elements[elements.length - 2];
    long linkId = Long.valueOf(sLinkId);
    Repository repository = RepositoryManager.getInstance().getRepository();
    repository.enableLink(linkId, enabled);
    return JsonBean.EMPTY_BEAN;
  }
}
