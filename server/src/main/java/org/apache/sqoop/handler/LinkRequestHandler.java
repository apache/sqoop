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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.apache.log4j.Logger;
import org.apache.sqoop.audit.AuditLoggerManager;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.ConnectorManager;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.json.JSONUtils;
import org.apache.sqoop.json.JsonBean;
import org.apache.sqoop.json.LinkBean;
import org.apache.sqoop.json.LinksBean;
import org.apache.sqoop.json.ValidationResultBean;
import org.apache.sqoop.model.ConfigUtils;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MPersistableEntity;
import org.apache.sqoop.model.MResource;
import org.apache.sqoop.repository.Repository;
import org.apache.sqoop.repository.RepositoryManager;
import org.apache.sqoop.security.authorization.AuthorizationEngine;
import org.apache.sqoop.security.AuthorizationManager;
import org.apache.sqoop.server.RequestContext;
import org.apache.sqoop.server.RequestHandler;
import org.apache.sqoop.server.common.ServerError;
import org.apache.sqoop.validation.ConfigValidationResult;
import org.json.simple.JSONObject;

public class LinkRequestHandler implements RequestHandler {

  private static final Logger LOG = Logger.getLogger(LinkRequestHandler.class);

  static final String ENABLE = "enable";
  static final String DISABLE = "disable";
  static final String LINKS_PATH = "links";
  static final String LINK_PATH = "link";

  public LinkRequestHandler() {
    LOG.info("LinkRequestHandler initialized");
  }

  @Override
  public JsonBean handleEvent(RequestContext ctx) {
    switch (ctx.getMethod()) {
    case GET:
      return getLinks(ctx);
    case POST:
      return createUpdateLink(ctx, true);
    case PUT:
      if (ctx.getLastURLElement().equals(ENABLE)) {
        return enableLink(ctx, true);
      } else if (ctx.getLastURLElement().equals(DISABLE)) {
        return enableLink(ctx, false);
      } else {
        return createUpdateLink(ctx, false);
      }
    case DELETE:
      return deleteLink(ctx);
    }

    return null;
  }

  /**
   * Delete link in the repository.
   *
   * @param ctx Context object
   * @return Empty bean
   */
  private JsonBean deleteLink(RequestContext ctx) {
    Repository repository = RepositoryManager.getInstance().getRepository();
    String linkIdentifier = ctx.getLastURLElement();
    // support linkName or linkId for the api
    long linkId = HandlerUtils.getLinkIdFromIdentifier(linkIdentifier, repository);

    // Authorization check
    AuthorizationEngine.deleteLink(String.valueOf(linkId));

    AuditLoggerManager.getInstance().logAuditEvent(ctx.getUserName(),
        ctx.getRequest().getRemoteAddr(), "delete", "link", linkIdentifier);

    repository.deleteLink(linkId);
    MResource resource = new MResource(String.valueOf(linkId), MResource.TYPE.LINK);
    AuthorizationManager.getAuthorizationHandler().removeResource(resource);
    return JsonBean.EMPTY_BEAN;
  }

  /**
   * Create or Update link in repository.
   *
   * @param ctx Context object
   * @return Validation bean object
   */
  private JsonBean createUpdateLink(RequestContext ctx, boolean create) {

    Repository repository = RepositoryManager.getInstance().getRepository();

    LinkBean linkBean = new LinkBean();
    try {
      JSONObject postData = JSONUtils.parse(ctx.getRequest().getReader());
      linkBean.restore(postData);
    } catch (IOException e) {
      throw new SqoopException(ServerError.SERVER_0003, "Can't read request content", e);
    }

    String username = ctx.getUserName();

    // Get link object
    List<MLink> links = linkBean.getLinks();
    if (links.size() != 1) {
      throw new SqoopException(ServerError.SERVER_0003,
          "Expected one link while parsing JSON request but got " + links.size());
    }

    MLink postedLink = links.get(0);

    // Authorization check
    if (create) {
      AuthorizationEngine.createLink(String.valueOf(postedLink.getConnectorId()));
    } else {
      AuthorizationEngine.updateLink(String.valueOf(postedLink.getConnectorId()),
              String.valueOf(postedLink.getPersistenceId()));
    }

    MLinkConfig linkConfig = ConnectorManager.getInstance()
        .getConnectorConfigurable(postedLink.getConnectorId()).getLinkConfig();
    if (!linkConfig.equals(postedLink.getConnectorLinkConfig())) {
      throw new SqoopException(ServerError.SERVER_0003, "Detected incorrect link config structure");
    }
    // if update get the link id from the request URI
    if (!create) {
      String linkIdentifier = ctx.getLastURLElement();
      // support linkName or linkId for the api
      long linkId = HandlerUtils.getLinkIdFromIdentifier(linkIdentifier, repository);
      if (postedLink.getPersistenceId() == MPersistableEntity.PERSISTANCE_ID_DEFAULT) {
        MLink existingLink = repository.findLink(linkId);
        postedLink.setPersistenceId(existingLink.getPersistenceId());
      }
    }
    // Associated connector for this link
    SqoopConnector connector = ConnectorManager.getInstance().getSqoopConnector(
        postedLink.getConnectorId());

    // Validate user supplied config data
    ConfigValidationResult connectorLinkConfigValidation = ConfigUtils.validateConfigs(postedLink
        .getConnectorLinkConfig().getConfigs(), connector.getLinkConfigurationClass());
    // Return back link validation result bean
    ValidationResultBean linkValidationBean = new ValidationResultBean(
        connectorLinkConfigValidation);

    // If we're good enough let's perform the action
    if (connectorLinkConfigValidation.getStatus().canProceed()) {
      if (create) {
        AuditLoggerManager.getInstance().logAuditEvent(ctx.getUserName(),
            ctx.getRequest().getRemoteAddr(), "create", "link",
            String.valueOf(postedLink.getPersistenceId()));
        postedLink.setCreationUser(username);
        postedLink.setLastUpdateUser(username);
        repository.createLink(postedLink);
        linkValidationBean.setId(postedLink.getPersistenceId());
      } else {
        AuditLoggerManager.getInstance().logAuditEvent(ctx.getUserName(),
            ctx.getRequest().getRemoteAddr(), "update", "link",
            String.valueOf(postedLink.getPersistenceId()));
        postedLink.setLastUpdateUser(username);
        repository.updateLink(postedLink);
      }
    }

    return linkValidationBean;
  }

  private JsonBean getLinks(RequestContext ctx) {
    String identifier = ctx.getLastURLElement();
    LinkBean linkBean;
    Locale locale = ctx.getAcceptLanguageHeader();
    Repository repository = RepositoryManager.getInstance().getRepository();

    // links by connector
    if (ctx.getParameterValue(CONNECTOR_NAME_QUERY_PARAM) != null) {
      identifier = ctx.getParameterValue(CONNECTOR_NAME_QUERY_PARAM);
      AuditLoggerManager.getInstance().logAuditEvent(ctx.getUserName(),
          ctx.getRequest().getRemoteAddr(), "get", "linksByConnector", identifier);
      if (repository.findConnector(identifier) != null) {
        long connectorId = repository.findConnector(identifier).getPersistenceId();
        List<MLink> linkList = repository.findLinksForConnector(connectorId);

        // Authorization check
        linkList = AuthorizationEngine.filterResource(MResource.TYPE.LINK, linkList);

        linkBean = createLinksBean(linkList, locale);
      } else {
        // this means name nor Id existed
        throw new SqoopException(ServerError.SERVER_0005, "Invalid connector: " + identifier
            + " name for links given");
      }
    } else
    // all links in the system
    if (ctx.getPath().contains(LINKS_PATH)
        || (ctx.getPath().contains(LINK_PATH) && identifier.equals("all"))) {
      AuditLoggerManager.getInstance().logAuditEvent(ctx.getUserName(),
          ctx.getRequest().getRemoteAddr(), "get", "links", "all");
      List<MLink> linkList = repository.findLinks();

      // Authorization check
      linkList = AuthorizationEngine.filterResource(MResource.TYPE.LINK, linkList);

      linkBean = createLinksBean(linkList, locale);
    }
    // link by Id
    else {
      AuditLoggerManager.getInstance().logAuditEvent(ctx.getUserName(),
          ctx.getRequest().getRemoteAddr(), "get", "link", identifier);

      long linkId = HandlerUtils.getLinkIdFromIdentifier(identifier, repository);
      MLink link = repository.findLink(linkId);

      // Authorization check
      AuthorizationEngine.readLink(String.valueOf(link.getPersistenceId()));

      linkBean = createLinkBean(Arrays.asList(link), locale);
    }
    return linkBean;
  }

  private LinkBean createLinkBean(List<MLink> links, Locale locale) {
    LinkBean linkBean = new LinkBean(links);
    addLink(links, locale, linkBean);
    return linkBean;
  }

  private LinksBean createLinksBean(List<MLink> links, Locale locale) {
    LinksBean linksBean = new LinksBean(links);
    addLink(links, locale, linksBean);
    return linksBean;
  }

  private void addLink(List<MLink> links, Locale locale, LinkBean bean) {
    // Add associated resources into the bean
    for (MLink link : links) {
      long connectorId = link.getConnectorId();
      if (!bean.hasConnectorConfigBundle(connectorId)) {
        bean.addConnectorConfigBundle(connectorId, ConnectorManager.getInstance()
            .getResourceBundle(connectorId, locale));
      }
    }
  }

  private JsonBean enableLink(RequestContext ctx, boolean enabled) {
    Repository repository = RepositoryManager.getInstance().getRepository();
    String[] elements = ctx.getUrlElements();
    String linkIdentifier = elements[elements.length - 2];
    long linkId = HandlerUtils.getLinkIdFromIdentifier(linkIdentifier, repository);

    // Authorization check
    AuthorizationEngine.enableDisableLink(String.valueOf(linkId));

    repository.enableLink(linkId, enabled);
    return JsonBean.EMPTY_BEAN;
  }
}