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
package org.apache.sqoop.server;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.ErrorCode;
import org.apache.sqoop.json.ThrowableBean;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.common.SqoopProtocolConstants;
import org.apache.sqoop.common.SqoopResponseCode;
import org.apache.sqoop.error.code.CoreError;
import org.apache.sqoop.json.JsonBean;

@SuppressWarnings("serial")
public class SqoopProtocolServlet extends HttpServlet {

  private static final Logger LOG =
      Logger.getLogger(SqoopProtocolServlet.class);

  @Override
  protected final void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    RequestContext rctx = new RequestContext(req, resp);

    try {
      JsonBean bean = handleGetRequest(rctx);
      if (bean != null) {
        sendSuccessResponse(rctx, bean);
      }
    } catch (Exception ex) {
      LOG.error("Exception in GET " + rctx.getPath(), ex);
      sendErrorResponse(rctx, ex);
    }
  }

  @Override
  protected final void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    RequestContext rctx = new RequestContext(req, resp);
    try {
      JsonBean bean = handlePostRequest(rctx);
      if (bean != null) {
        sendSuccessResponse(rctx, bean);
      }
    } catch (Exception ex) {
      LOG.error("Exception in POST " + rctx.getPath(), ex);
      sendErrorResponse(rctx, ex);
    }
  }

  @Override
  protected final void doPut(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    RequestContext rctx = new RequestContext(req, resp);

    try {
      JsonBean bean = handlePutRequest(rctx);
      if (bean != null) {
        sendSuccessResponse(rctx, bean);
      }
    } catch (Exception ex) {
      LOG.error("Exception in PUT " + rctx.getPath(), ex);
      sendErrorResponse(rctx, ex);
    }
  }

  @Override
  protected final void doDelete(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    RequestContext rctx = new RequestContext(req, resp);

    try {
      JsonBean bean = handleDeleteRequest(rctx);
      if (bean != null) {
        sendSuccessResponse(rctx, bean);
      }
    } catch (Exception ex) {
      LOG.error("Exception in DELETE " + rctx.getPath(), ex);
      sendErrorResponse(rctx, ex);
    }
  }

  private void sendSuccessResponse(RequestContext ctx, JsonBean bean)
      throws IOException {
    HttpServletResponse response = ctx.getResponse();
    response.setStatus(HttpServletResponse.SC_OK);
    setContentType(response);
    setHeaders(response, SqoopResponseCode.SQOOP_1000);
    String responseString = bean.extract(true).toJSONString();
    response.getWriter().write(responseString);
    response.getWriter().flush();
  }

  private void sendErrorResponse(RequestContext ctx, Exception ex)
    throws IOException
  {
    HttpServletResponse response = ctx.getResponse();
    setContentType(response);
    setHeaders(response, SqoopResponseCode.SQOOP_2000);

    if (ex != null) {
      ErrorCode ec = null;
      if (ex instanceof SqoopException) {
        ec = ((SqoopException) ex).getErrorCode();
      } else {
        ec = CoreError.CORE_0000;
      }

      response.setHeader(
          SqoopProtocolConstants.HEADER_SQOOP_INTERNAL_ERROR_CODE,
          ec.getCode());

      response.setHeader(
          SqoopProtocolConstants.HEADER_SQOOP_INTERNAL_ERROR_MESSAGE,
          ex.getMessage());

      ThrowableBean throwableBean = new ThrowableBean(ex);

      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      response.getWriter().write(throwableBean.extract(true).toJSONString());
    } else {
      response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }

  }

  private void setContentType(HttpServletResponse response) {
    response.setContentType(SqoopProtocolConstants.JSON_CONTENT_TYPE);
  }

  private void setHeaders(HttpServletResponse response, SqoopResponseCode code)
  {
    response.setHeader(SqoopProtocolConstants.HEADER_SQOOP_ERROR_CODE,
        code.getCode());
    response.setHeader(SqoopProtocolConstants.HEADER_SQOOP_ERROR_MESSAGE,
        code.getMessage());
  }

  protected JsonBean handleGetRequest(RequestContext ctx) throws Exception {
    super.doGet(ctx.getRequest(), ctx.getResponse());

    return null;
  }

  protected JsonBean handlePostRequest(RequestContext ctx) throws Exception {
    super.doPost(ctx.getRequest(), ctx.getResponse());

    return null;
  }

  protected JsonBean handlePutRequest(RequestContext ctx) throws Exception {
    super.doPut(ctx.getRequest(), ctx.getResponse());

    return null;
  }

  protected JsonBean handleDeleteRequest(RequestContext ctx) throws Exception {
    super.doDelete(ctx.getRequest(), ctx.getResponse());

    return null;
  }
}
