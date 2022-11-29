/**
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

import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
        .OMResponse;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.Nonnull;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;

/**
 * Base class for responses that need to move keys from an arbitrary table to
 * the deleted table.
 */
@CleanupTableInfo(cleanupTables = {DELETED_TABLE})
public abstract class AbstractOMKeyDeleteResponse extends OmKeyResponse {
  private static final Logger LOG =
          LoggerFactory.getLogger(AbstractOMKeyDeleteResponse.class);

  // The key in delete table, *not* in key/file table
  private String deleteKey;
  protected RepeatedOmKeyInfo repeatedOmKeyInfo;

  public AbstractOMKeyDeleteResponse(
      @Nonnull OMResponse omResponse, @Nonnull String deleteKey,
      @Nonnull RepeatedOmKeyInfo repeatedOmKeyInfo) {

    super(omResponse);
    this.deleteKey = deleteKey;
    this.repeatedOmKeyInfo = repeatedOmKeyInfo;
  }

  public AbstractOMKeyDeleteResponse(@Nonnull OMResponse omResponse,
      @Nonnull String deleteKey,
      @Nonnull RepeatedOmKeyInfo repeatedOmKeyInfo, BucketLayout bucketLayout) {

    super(omResponse, bucketLayout);
    this.deleteKey = deleteKey;
    this.repeatedOmKeyInfo = repeatedOmKeyInfo;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public AbstractOMKeyDeleteResponse(@Nonnull OMResponse omResponse,
                                     @Nonnull BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
    checkStatusNotOK();
  }

  /**
   * An abstract method to retrieve the key in key table for deletion,
   * depending on the bucket layout.
   */
  protected abstract String getKeyToDelete(
      OMMetadataManager omMetadataManager, OmKeyInfo omKeyInfo);

  /**
   * Adds the operation of deleting the {@code keyName omKeyInfo} pair from
   * {@code fromTable} to the batch operation {@code batchOperation}. The
   * batch operation is not committed, so no changes are persisted to disk.
   *
   * @param omMetadataManager
   * @param batchOperation
   */
  protected void deleteFromKeyTable(
      OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    if (repeatedOmKeyInfo.getOmKeyInfoList().isEmpty()) {
      return;
    }
    Table<String, OmKeyInfo> keyTable =
        omMetadataManager.getKeyTable(getBucketLayout());
    for (OmKeyInfo omKeyInfo :
        repeatedOmKeyInfo.getOmKeyInfoList()) {
      String key = getKeyToDelete(omMetadataManager, omKeyInfo);
      keyTable.deleteWithBatch(batchOperation, key);
    }
  }

  protected void insertToDeleteTable(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    String key = deleteKey;

    // Making sure that the corresponding UpdateID has no duplication (no
    // implicit block leak). This check should be cheap because RocksDB
    // internally maintains a Bloom filter.
    if (omMetadataManager.getDeletedTable().isExist(key)) {
      LOG.error("An entry already exists in delete table for key {}", key);
      throw new IllegalStateException("An entry already exists in delete"
          + " table: " + key);
    }

    // No need to collect blocks for keys without any blocks. Filter them
    // out and don't put them into delete table.
    RepeatedOmKeyInfo repeatedOmKeyInfoWithBlocks = new RepeatedOmKeyInfo(
        repeatedOmKeyInfo.getOmKeyInfoList().stream().filter(
            (omKeyInfo1 -> !isKeyEmpty(omKeyInfo1))
        ).collect(Collectors.toList()));

    // If all key info do not have blocks, no need to put to delete table.
    if (repeatedOmKeyInfoWithBlocks.getOmKeyInfoList().isEmpty()) {
      return;
    }
    omMetadataManager.getDeletedTable().putWithBatch(
        batchOperation, key, repeatedOmKeyInfoWithBlocks);
  }

  protected RepeatedOmKeyInfo getRepeatedOmKeyInfo() {
    return repeatedOmKeyInfo;
  }
  @Override
  public abstract void addToDBBatch(OMMetadataManager omMetadataManager,
        BatchOperation batchOperation) throws IOException;

  /**
   * Check if the key is empty or not. Key will be empty if it does not have
   * blocks.
   *
   * @param keyInfo
   * @return if empty true, else false.
   */
  protected boolean isKeyEmpty(@Nullable OmKeyInfo keyInfo) {
    if (keyInfo == null) {
      return true;
    }
    for (OmKeyLocationInfoGroup keyLocationList : keyInfo
            .getKeyLocationVersions()) {
      if (keyLocationList.getLocationListCount() != 0) {
        return false;
      }
    }
    return true;
  }
}
