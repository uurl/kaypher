/*
 * Copyright 2019 Treu Techologies
 *
 * See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.treutec.kaypher.links;

public final class DocumentationLinks {

  private static final String CONFLUENT_DOCS_ROOT_URL = "https://docs.treutec.io/current/";

  /*
  KAYPHER
   */
  private static final String KAYPHER_DOCS_ROOT_URL = CONFLUENT_DOCS_ROOT_URL + "kaypher/docs/";

  private static final String SECURITY_DOCS_URL = KAYPHER_DOCS_ROOT_URL
      + "installation/server-config/security.html";

  public static final String SECURITY_CLI_SSL_DOC_URL = SECURITY_DOCS_URL
      + "#configuring-cli-for-https";

  /*
  Schema Registry
   */
  private static final String SCHEMA_REGISTRY_DOCS_ROOT_URL = CONFLUENT_DOCS_ROOT_URL
      + "schema-registry/docs/";

  public static final String SR_SERIALISER_DOC_URL = SCHEMA_REGISTRY_DOCS_ROOT_URL
      + "serializer-formatter.html";

  private static final String SCHEMA_REGISTRY_API_DOC_URL =
      SCHEMA_REGISTRY_DOCS_ROOT_URL + "api.html";

  public static final String SR_REST_GETSUBJECTS_DOC_URL = SCHEMA_REGISTRY_API_DOC_URL
      + "#get--subjects";

  private DocumentationLinks() {
  }
}
