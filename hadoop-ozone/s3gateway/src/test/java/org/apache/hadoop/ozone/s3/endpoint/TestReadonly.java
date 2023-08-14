/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.hadoop.ozone.s3.endpoint;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.s3.S3GatewayConfigKeys;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.format.DateTimeFormatter;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Utils.urlEncode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

/**
 * Test put object.
 */
public class TestReadonly {
  public static final String CONTENT = "0123456789";
  private String bucketName = "b1";
  private String keyName = "key=value/1";
  private String destBucket = "b2";
  private String destkey = "key=value/2";
  private String nonexist = "nonexist";
  private OzoneClient clientStub;
  private ObjectEndpoint objectEndpoint;

  private HttpHeaders headers;
  private ByteArrayInputStream body;
  private ContainerRequestContext context;

  @Before
  public void setup() throws IOException {
    //Create client stub and object store stub.
    clientStub = new OzoneClientStub();

    // Create bucket
    clientStub.getObjectStore().createS3Bucket(bucketName);
    clientStub.getObjectStore().createS3Bucket(destBucket);

    /*
    OzoneBucket bucket = clientStub.getObjectStore().getS3Bucket(bucketName);
    OzoneOutputStream keyStream =
              bucket.createKey("key1", CONTENT.getBytes(UTF_8).length);
    keyStream.write(CONTENT.getBytes(UTF_8));
    keyStream.close();
*/
    // Create PutObject and setClient to OzoneClientStub
    objectEndpoint = new ObjectEndpoint();
    objectEndpoint.setClient(clientStub);
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(S3GatewayConfigKeys.OZONE_S3G_READONLY, true);
    objectEndpoint.setOzoneConfiguration(conf);

    // Return data for get
    /*
    headers = Mockito.mock(HttpHeaders.class);
    objectEndpoint.setHeaders(headers);
    body = new ByteArrayInputStream(CONTENT.getBytes(UTF_8));

    context = Mockito.mock(ContainerRequestContext.class);
    Mockito.when(context.getUriInfo()).thenReturn(Mockito.mock(UriInfo.class));
    Mockito.when(context.getUriInfo().getQueryParameters())
            .thenReturn(new MultivaluedHashMap<>());
    objectEndpoint.setContext(context);
     */
  }

  @Test
  public void testPutObject() throws IOException, OS3Exception {
    //GIVEN
    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    objectEndpoint.setHeaders(headers);

    //WHEN
    Response response = objectEndpoint.put(bucketName, keyName, CONTENT
        .length(), 1, null, body);

    Assert.assertEquals(Response.Status.METHOD_NOT_ALLOWED, response.getStatus());
  }

  @Test
  public void get() throws IOException, OS3Exception {
    //WHEN
    Response response = objectEndpoint.get(bucketName, "key1", null, 0, null, body);

    //THEN
    OzoneInputStream ozoneInputStream =
            clientStub.getObjectStore().getS3Bucket("b1")
                    .readKey("key1");
    String keyContent =
            IOUtils.toString(ozoneInputStream, UTF_8);

    Assert.assertEquals(CONTENT, keyContent);
    Assert.assertEquals("" + keyContent.length(),
            response.getHeaderString("Content-Length"));

    DateTimeFormatter.RFC_1123_DATE_TIME
            .parse(response.getHeaderString("Last-Modified"));
  }

  //@Test
  public void testCopyObject() throws IOException, OS3Exception {
    // Put object in to source bucket
    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    objectEndpoint.setHeaders(headers);
    keyName = "sourceKey";

    Response response = objectEndpoint.put(bucketName, keyName,
        CONTENT.length(), 1, null, body);
    Assert.assertEquals(Response.Status.METHOD_NOT_ALLOWED, response.getStatus());

  }
}
