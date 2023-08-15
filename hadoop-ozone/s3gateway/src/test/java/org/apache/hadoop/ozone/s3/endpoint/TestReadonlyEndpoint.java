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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.ObjectStoreStub;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.s3.S3GatewayConfigKeys;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;;
import javax.ws.rs.core.UriInfo;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.format.DateTimeFormatter;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Test put object.
 */
public class TestReadonlyEndpoint {
  public static final String CONTENT = "0123456789";
  private String bucketName = "b1";
  private String keyName = "k1/1";
  private String destBucket = "b2";
  private OzoneClient clientStub;
  private ObjectEndpoint objectEndpoint;
  private BucketEndpoint bucketEndpoint;

  private HttpHeaders headers;
  private ByteArrayInputStream body;
  private ContainerRequestContext context;

  @Before
  public void setup() throws IOException {
    //Create client stub and object store stub.
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(S3GatewayConfigKeys.OZONE_S3G_READONLY, true);
    ObjectStoreStub objectStoreStub = new ObjectStoreStub();
    clientStub = new OzoneClientStub(objectStoreStub, conf);

    // Create bucket
    clientStub.getObjectStore().createS3Bucket(bucketName);
    clientStub.getObjectStore().createS3Bucket(destBucket);

    // Create PutObject and setClient to OzoneClientStub
    objectEndpoint = new ObjectEndpoint();
    objectEndpoint.setClient(clientStub);
    objectEndpoint.setOzoneConfiguration(new OzoneConfiguration());

    OzoneBucket bucket = clientStub.getObjectStore().getS3Bucket(bucketName);
    OzoneOutputStream keyStream =
        bucket.createKey("key1", CONTENT.getBytes(UTF_8).length);
    keyStream.write(CONTENT.getBytes(UTF_8));
    keyStream.close();

    // Return data for get
    headers = Mockito.mock(HttpHeaders.class);
    objectEndpoint.setHeaders(headers);
    body = new ByteArrayInputStream(CONTENT.getBytes(UTF_8));

    context = Mockito.mock(ContainerRequestContext.class);
    Mockito.when(context.getUriInfo()).thenReturn(Mockito.mock(UriInfo.class));
    Mockito.when(context.getUriInfo().getQueryParameters())
        .thenReturn(new MultivaluedHashMap<>());
    objectEndpoint.setContext(context);

    bucketEndpoint = new BucketEndpoint();
    bucketEndpoint.setClient(clientStub);
  }

  // Put should fail when configured as readonly
  @Test
  public void testPutObject() throws IOException, OS3Exception {
    Response response = objectEndpoint.put(bucketName, keyName, CONTENT
        .length(), 1, null, body);

    Assert.assertEquals(Status.METHOD_NOT_ALLOWED.getStatusCode(),
        response.getStatus());
  }

  // Get should succeed
  @Test
  public void get() throws IOException, OS3Exception {
    //WHEN
    Response response = objectEndpoint.get(bucketName, "key1",
        null, 0, null, body);

    //THEN
    OzoneInputStream ozoneInputStream =
            clientStub.getObjectStore().getS3Bucket(bucketName)
                    .readKey("key1");
    String keyContent =
            IOUtils.toString(ozoneInputStream, UTF_8);

    Assert.assertEquals(CONTENT, keyContent);
    Assert.assertEquals("" + keyContent.length(),
        response.getHeaderString("Content-Length"));

    DateTimeFormatter.RFC_1123_DATE_TIME
        .parse(response.getHeaderString("Last-Modified"));
  }

  // Copy is also treated as write
  @Test
  public void testCopyObject() throws IOException, OS3Exception {
    // Put object in to source bucket
    objectEndpoint.setHeaders(headers);

    Response response = objectEndpoint.put(bucketName, keyName,
        CONTENT.length(), 1, null, body);
    Assert.assertEquals(Status.METHOD_NOT_ALLOWED.getStatusCode(),
        response.getStatus());
  }

  @Test
  public void testDelete() throws IOException, OS3Exception {
    //GIVEN
    OzoneBucket bucket =
        clientStub.getObjectStore().getS3Bucket("b1");

    bucket.createKey("key1", 0).close();

    //WHEN
    Response response = objectEndpoint.delete("b1", "key1", null);

    //THEN
    Assert.assertEquals(Status.METHOD_NOT_ALLOWED.getStatusCode(),
        response.getStatus());
  }

  @Test
  public void testBucketPut() throws IOException, OS3Exception {
    Response response = bucketEndpoint.put(bucketName, null, null, null);
    Assert.assertEquals(Status.METHOD_NOT_ALLOWED.getStatusCode(),
        response.getStatus());
  }

  @Test
  public void listDir() throws OS3Exception, IOException {
    Response response =
        bucketEndpoint.get(bucketName, "/", null, null, 100,
                    "dir1", null, null, null, null, null);

    Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
  }

  @Test
  public void testInitiateMultipartUpload() throws Exception {
    Response response = objectEndpoint.initializeMultipartUpload(bucketName,
        keyName);

    Assert.assertEquals(Status.METHOD_NOT_ALLOWED.getStatusCode(),
            response.getStatus());
  }
}
