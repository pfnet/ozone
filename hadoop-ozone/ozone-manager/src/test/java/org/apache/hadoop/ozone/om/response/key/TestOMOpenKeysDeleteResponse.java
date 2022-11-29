/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.response.key;

import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.util.Time;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.addBucketToDB;

/**
 * Tests OMOpenKeysDeleteResponse.
 */
@RunWith(Parameterized.class)
public class TestOMOpenKeysDeleteResponse extends TestOMKeyResponse {
  private static final long KEY_LENGTH = 100;
  private final BucketLayout bucketLayout;

  public TestOMOpenKeysDeleteResponse(BucketLayout bucketLayout) {
    this.bucketLayout = bucketLayout;
  }

  @Override
  public BucketLayout getBucketLayout() {
    return bucketLayout;
  }

  @Parameters
  public static Collection<BucketLayout> bucketLayouts() {
    return Arrays.asList(
        BucketLayout.DEFAULT,
        BucketLayout.FILE_SYSTEM_OPTIMIZED
    );
  }

  /**
   * Tests deleting a subset of keys from the open key table DB when the keys
   * have no associated block data.
   */
  @Test
  public void testAddToDBBatchWithEmptyBlocks() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager, getBucketLayout());
    Map<String, OmKeyInfo> keysToDelete = addOpenKeysToDB(volumeName, 3);
    Map<String, OmKeyInfo> keysToKeep = addOpenKeysToDB(volumeName, 3);

    String delKey = OmUtils.keyForDeleteTable(Time.now(), 360L);

    createAndCommitResponse(keysToDelete, delKey, Status.OK);

    RepeatedOmKeyInfo deletedKeyInfos =
            omMetadataManager.getDeletedTable().get("0");
    // Nothing should be in delete table, as all blocks are empty.
    Assert.assertNull(deletedKeyInfos);

    for (Map.Entry<String, OmKeyInfo> entry: keysToDelete.entrySet()) {
      // open keys with no associated block data should have been removed
      // from the open key table.
      Assert.assertFalse(omMetadataManager.getOpenKeyTable(getBucketLayout())
          .isExist(entry.getKey()));
    }

    for (Map.Entry<String, OmKeyInfo> entry : keysToKeep.entrySet()) {
      // These keys should not have been removed from the open key table.
      Assert.assertTrue(omMetadataManager.getOpenKeyTable(getBucketLayout())
          .isExist(entry.getKey()));
    }

    // Empty blocks won't be written to delete table
    Assert.assertFalse(omMetadataManager.getDeletedTable().isExist(delKey));
  }

  /**
   * Tests deleting a subset of keys from the open key table DB when the keys
   * have associated block data.
   */
  @Test
  public void testAddToDBBatchWithNonEmptyBlocks() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager, getBucketLayout());
    Map<String, OmKeyInfo> keysToDelete = addOpenKeysToDB(volumeName, 3,
        KEY_LENGTH);
    Map<String, OmKeyInfo> keysToKeep = addOpenKeysToDB(volumeName, 3,
        KEY_LENGTH);

    String delKey = OmUtils.keyForDeleteTable(Time.now(), 360L);
    createAndCommitResponse(keysToDelete, delKey, Status.OK);

    // UpdateID in keysToDelete is zero by default. Timestamp is the value
    // set in the mock above.
    RepeatedOmKeyInfo deletedKeyInfos =
        omMetadataManager.getDeletedTable().get(delKey);
    Assert.assertNotNull(deletedKeyInfos);
    Assert.assertEquals(keysToDelete.size(),
        deletedKeyInfos.getOmKeyInfoList().size());

    for (Map.Entry<String, OmKeyInfo> entry: keysToDelete.entrySet()) {
      // These keys should have been moved from the open key table to the
      // delete table.
      Assert.assertFalse(omMetadataManager.getOpenKeyTable(getBucketLayout())
          .isExist(entry.getKey()));
      Assert.assertTrue(containsKey(deletedKeyInfos, entry.getValue()));
    }

    for (Map.Entry<String, OmKeyInfo> entry : keysToKeep.entrySet()) {
      // These keys should not have been moved out of the open key table.
      Assert.assertTrue(omMetadataManager.getOpenKeyTable(getBucketLayout())
          .isExist(entry.getKey()));
      Assert.assertFalse(containsKey(deletedKeyInfos, entry.getValue()));
    }

    // Deletion (insert to delete table) should be committed as well.
    Assert.assertTrue(omMetadataManager.getDeletedTable().isExist(delKey));
  }

  public boolean containsKey(RepeatedOmKeyInfo repeatedOmKeyInfo,
      OmKeyInfo omKeyInfo) {
    for (OmKeyInfo omKeyInfo1 : repeatedOmKeyInfo.getOmKeyInfoList()) {
      if (omKeyInfo.getVolumeName().equals(omKeyInfo1.getVolumeName()) &&
          omKeyInfo.getBucketName().equals(omKeyInfo1.getBucketName()) &&
          omKeyInfo.getKeyName().equals(omKeyInfo1.getKeyName())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Tests attempting deleting keys from the open key table DB when the
   * submitted response has an error status. In this case, no changes to the
   * DB should be made.
   */
  @Test
  public void testAddToDBBatchWithErrorResponse() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager, getBucketLayout());
    Map<String, OmKeyInfo> keysToDelete = addOpenKeysToDB(volumeName, 3);

    String delKey = OmUtils.keyForDeleteTable(Time.now(), 360L);

    createAndCommitResponse(keysToDelete, delKey, Status.INTERNAL_ERROR);

    RepeatedOmKeyInfo deletedKeyInfos =
            omMetadataManager.getDeletedTable().get(delKey);
    Assert.assertNull(deletedKeyInfos);

    for (String key: keysToDelete.keySet()) {
      // If an error occurs in the response, the batch operation moving keys
      // from the open key table to the delete table should not be committed.
      Assert.assertTrue(
          omMetadataManager.getOpenKeyTable(getBucketLayout()).isExist(key));
    }

    // Deletion (insert to delete table) should not be committed as well.
    Assert.assertFalse(omMetadataManager.getDeletedTable().isExist(delKey));
  }

  /**
   * Constructs an {@link OMOpenKeysDeleteResponse} to delete the keys in
   * {@code keysToDelete}, with the completion status set to {@code status}.
   * If {@code status} is {@link Status#OK}, the keys to delete will be added
   * to a batch operation and committed to the database.
   * @throws Exception
   */
  private void createAndCommitResponse(Map<String, OmKeyInfo> keysToDelete,
      String deleteKey, Status status) throws Exception {

    OMResponse omResponse = OMResponse.newBuilder()
        .setStatus(status)
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteOpenKeys)
        .build();

    OMOpenKeysDeleteResponse response = new OMOpenKeysDeleteResponse(omResponse,
      deleteKey, keysToDelete, getBucketLayout());

    // Operations are only added to the batch by this method when status is OK.
    response.checkAndUpdateDB(omMetadataManager, batchOperation);

    // If status is not OK, this will do nothing.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
  }

  /**
   * Creates {@code numKeys} open keys with random names, maps each one to a
   * new {@link OmKeyInfo} object, adds them to the open key table cache, and
   * returns them. These keys will have no associated block data.
   */
  private Map<String, OmKeyInfo> addOpenKeysToDB(String volume, int numKeys)
      throws Exception {
    return addOpenKeysToDB(volume, numKeys, 0);
  }

  /**
   * Creates {@code numKeys} open keys with random names, maps each one to a
   * new {@link OmKeyInfo} object, adds them to the open key table cache, and
   * returns them.
   * If {@code keyLength} is greater than 0, adds one block with that many
   * bytes of data for each key.
   * @throws Exception
   */
  private Map<String, OmKeyInfo> addOpenKeysToDB(String volume, int numKeys,
      long keyLength) throws Exception {

    Map<String, OmKeyInfo> newOpenKeys = new HashMap<>();

    for (int i = 0; i < numKeys; i++) {
      String bucket = UUID.randomUUID().toString();
      String key = UUID.randomUUID().toString();
      addBucketToDB(volume, bucket, omMetadataManager, getBucketLayout());
      long clientID = random.nextLong();
      long parentID = random.nextLong();

      OmKeyInfo omKeyInfo = OMRequestTestUtils.createOmKeyInfo(volume,
          bucket, key, replicationType, replicationFactor);

      if (keyLength > 0) {
        OMRequestTestUtils.addKeyLocationInfo(omKeyInfo, 0, keyLength);
      }

      final String openKey;

      // Add to the open key table DB, not cache.
      // In a real execution, the open key would have been removed from the
      // cache by the request, and it would only remain in the DB.
      if (getBucketLayout().isFileSystemOptimized()) {
        String file = OzoneFSUtils.getFileName(key);
        final long volumeId = omMetadataManager.getVolumeId(volume);
        final long bucketId = omMetadataManager.getBucketId(volume, bucket);
        omKeyInfo.setFileName(file);
        omKeyInfo.setParentObjectID(parentID);
        OMRequestTestUtils.addFileToKeyTable(true, false, file, omKeyInfo,
            clientID, 0L, omMetadataManager);
        openKey = omMetadataManager.getOpenFileName(
                volumeId, bucketId, parentID, file, clientID);
      } else {
        OMRequestTestUtils.addKeyToTable(true, false, omKeyInfo,
            clientID, 0L, omMetadataManager);
        openKey = omMetadataManager.getOpenKey(volume, bucket, key, clientID);
      }
      Assert.assertTrue(omMetadataManager.getOpenKeyTable(getBucketLayout())
          .isExist(openKey));

      newOpenKeys.put(openKey, omKeyInfo);
    }

    return newOpenKeys;
  }
}
