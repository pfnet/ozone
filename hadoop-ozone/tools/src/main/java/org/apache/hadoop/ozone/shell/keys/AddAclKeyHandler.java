/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.shell.keys;

import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.acl.AclHandler;
import org.apache.hadoop.ozone.shell.acl.AclOption;
import org.apache.hadoop.ozone.shell.prefix.PrefixUri;
import picocli.CommandLine;

import java.io.IOException;
import java.util.Iterator;

/**
 * Add ACL to key.
 */
@CommandLine.Command(name = AclHandler.ADD_ACL_NAME,
    description = AclHandler.ADD_ACL_DESC)
public class AddAclKeyHandler extends AclHandler {

  @CommandLine.Mixin
  private KeyUri address;

  @CommandLine.Mixin
  private AclOption acls;

  @CommandLine.Mixin
  private PrefixUri prefix;

  @Override
  protected OzoneAddress getAddress() {
    return address.getValue();
  }

  @Override
  protected void execute(OzoneClient client, OzoneObj obj) throws IOException {
    if (this.prefix == null) {
      acls.addTo(obj, client.getObjectStore(), out());
    } else {
      String prevKey = null;
      OzoneBucket bucket = client.getObjectStore()
          .getS3Bucket(this.prefix.getValue().getBucketName());
      Iterator<? extends OzoneKey> keyIter =
          bucket.listKeys(obj.getPrefixName(),
              "", prevKey);
      long startAt = System.nanoTime();
      long success = 0;
      long error = 0;
      while (keyIter.hasNext()) {
        OzoneKey next = keyIter.next();
        OzoneObj ozoneObj = OzoneObjInfo.Builder.newBuilder()
            .setBucketName(next.getBucketName())
            .setVolumeName(next.getVolumeName())
            .setKeyName(next.getName())
            .setResType(OzoneObj.ResourceType.KEY)
            .setStoreType(OzoneObj.StoreType.OZONE)
            .build();
        try {
          acls.addTo(ozoneObj, client.getObjectStore(), out());
        } catch (IOException e) {
          out().printf("%s: %s", next.getName(), e.getMessage());
          error += 1;
        }
        success += 1;
      }
      out().printf("%d key changes completed (%d errors) in %d seconds.\n", success, error,
              (System.nanoTime() - startAt) / 1000 / 1000 / 1000);
    }
  }

}
