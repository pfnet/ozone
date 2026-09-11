/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.server.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.eclipse.jetty.http.HttpCompliance;
import org.eclipse.jetty.http.HttpComplianceSection;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpParser;
import org.eclipse.jetty.http.HttpVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Regression tests for HDDS-5271.
 *
 * <p>Jetty's {@code HttpParser.CACHE} is a case-insensitive trie holding
 * canonical spellings of common Content-Type values. Left enabled, the parser
 * replaces the value on the wire with the cached spelling, which breaks AWS
 * SigV4 for every client that signs a Content-Type carrying a lowercase
 * charset. {@link HttpServer2} drops the responsible compliance section; these
 * tests pin that behaviour down.
 */
public class TestHttpServer2HeaderValueFidelity {

  /**
   * Forces HttpServer2 class initialization, which is what applies the fix.
   *
   * <p>Reading a constant such as {@code HttpServer2.HTTP_SCHEME} would not do:
   * the compiler inlines compile-time constants, so the reference never
   * triggers initialization.
   */
  private static void loadHttpServer2() {
    try {
      Class.forName(HttpServer2.class.getName(), true,
          HttpServer2.class.getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new AssertionError(e);
    }
  }

  @Test
  public void testCaseInsensitiveFieldValueCacheIsDisabled() {
    loadHttpServer2();
    assertFalse(
        HttpCompliance.RFC7230.sections()
            .contains(HttpComplianceSection.CASE_INSENSITIVE_FIELD_VALUE_CACHE),
        "HttpServer2 must disable Jetty's case-insensitive field value cache;"
            + " leaving it on rewrites signed Content-Type headers"
            + " (HDDS-5271)");
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "text/plain; charset=utf-8",
      "text/plain;charset=utf-8",
      "text/html; charset=utf-8",
      "text/xml; charset=utf-8",
      "text/json; charset=utf-8",
      "application/json; charset=utf-8",
      "application/x-www-form-urlencoded; charset=utf-8",
      "text/plain; charset=iso-8859-1",
      "text/plain; charset=UTF-8",
      "TEXT/PLAIN; charset=utf-8",
      "text/plain; charset=utf-8; foo=bar",
      "text/csv; charset=utf-8",
      "application/octet-stream; charset=utf-8",
  })
  public void testContentTypeReachesHandlerVerbatim(String contentType) {
    loadHttpServer2();

    List<HttpField> parsed = new ArrayList<>();
    HttpParser parser = new HttpParser(new CapturingHandler(parsed),
        HttpCompliance.RFC7230);

    String request = "PUT /bucket/key HTTP/1.1\r\n"
        + "Host: s3g.example.com\r\n"
        + "Content-Type: " + contentType + "\r\n"
        + "Content-Length: 0\r\n"
        + "\r\n";
    parser.parseNext(
        ByteBuffer.wrap(request.getBytes(StandardCharsets.ISO_8859_1)));

    String seen = parsed.stream()
        .filter(f -> HttpHeader.CONTENT_TYPE.is(f.getName()))
        .map(HttpField::getValue)
        .findFirst()
        .orElse(null);

    assertEquals(contentType, seen,
        "Content-Type must reach the application exactly as sent, otherwise"
            + " the SigV4 canonical request cannot be rebuilt (HDDS-5271)");
  }

  private static final class CapturingHandler
      implements HttpParser.RequestHandler {

    private final List<HttpField> fields;

    private CapturingHandler(List<HttpField> fields) {
      this.fields = fields;
    }

    @Override
    public boolean startRequest(String method, String uri,
        HttpVersion version) {
      return false;
    }

    @Override
    public void parsedHeader(HttpField field) {
      fields.add(field);
    }

    @Override
    public boolean headerComplete() {
      return false;
    }

    @Override
    public boolean content(ByteBuffer item) {
      return false;
    }

    @Override
    public boolean contentComplete() {
      return false;
    }

    @Override
    public boolean messageComplete() {
      return true;
    }

    @Override
    public void earlyEOF() {
    }

    /**
     * Zero disables the per-connection field cache, so the test exercises the
     * static {@code HttpParser.CACHE} that HDDS-5271 is about.
     */
    @Override
    public int getHeaderCacheSize() {
      return 0;
    }
  }
}
