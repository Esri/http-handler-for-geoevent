/*
  Copyright 1995-2016 Esri

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

  For additional information, contact:
  Environmental Systems Research Institute, Inc.
  Attn: Contracts Dept
  380 New York Street
  Redlands, California, USA 92373

  email: contracts@esri.com
 */

package com.esri.geoevent.processor.httpHandler;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.http.GeoEventHttpClientService;
import com.esri.ges.manager.geoeventdefinition.GeoEventDefinitionManager;
import com.esri.ges.messaging.Messaging;
import com.esri.ges.processor.GeoEventProcessor;
import com.esri.ges.processor.GeoEventProcessorServiceBase;

public class HttpHandlerService extends GeoEventProcessorServiceBase
{
  static GeoEventHttpClientService  httpClientService;
  private Messaging                 messaging;
  private GeoEventDefinitionManager geoEventDefinitionManager;

  public HttpHandlerService()
  {
    definition = new HttpHandlerDefinition();
  }

  @Override
  public GeoEventProcessor create() throws ComponentException
  {
    HttpHandler httpHndl = new HttpHandler(definition);
    httpHndl.setMessaging(messaging);
    httpHndl.setGeoEventDefinitionManager(geoEventDefinitionManager);
    return httpHndl;
  }

  public void setMessaging(Messaging messaging)
  {
    this.messaging = messaging;
  }

  public void setHttpClientService(GeoEventHttpClientService httpClientService)
  {
    HttpHandlerService.httpClientService = httpClientService;
  }

  public void setGeoEventDefinitionManager(GeoEventDefinitionManager geoEventDefinitionManager)
  {
    this.geoEventDefinitionManager = geoEventDefinitionManager;
  }
}
