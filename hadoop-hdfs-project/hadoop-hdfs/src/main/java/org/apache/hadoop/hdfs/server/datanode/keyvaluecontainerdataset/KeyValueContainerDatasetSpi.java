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

package org.apache.hadoop.hdfs.server.datanode.keyvaluecontainerdataset;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.datanode.dataset.*;
import org.apache.hadoop.hdfs.server.datanode.keyvaluecontainerdataset.exceptions.*;
import org.apache.hadoop.storagecontainer.protocol.StorageContainer;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Set;
import java.util.UUID;

/**
 * Interface abstracting storage operations to support containers. Exposes
 * the following two storage primitives:
 *
 *  1. Key-value containers that support atomic put, delete and replace and
 *     support lexical iteration. The length of individual keys and values
 *     depends on the specific implementation but it is expected to be
 *     limited to a few KB.
 *
 *  2. Named blobs which can be up to a few GB in size. The exact limit is
 *     left to implementors. Blobs support streaming writes but not atomic
 *     puts.
 */
@InterfaceAudience.Private
public interface KeyValueContainerDatasetSpi<V extends VolumeSpi>
    extends DatasetSpi<VolumeSpi> {

  /**
   * Create a key-value container.
   *
   * @param containerKey key identifying the container. The id, generation
   *                     stamp and blockpoolId fields must be initialized.
   *
   * @return a reference to the newly created container.
   *
   * @throws ContainerAlreadyExistsException if the container with a matching
   *                                         [id+blockpoolId] combination is
   *                                         already present.
   * @throws IOException if the operation failed for any other reason.
   */
  KeyValueContainer createContainer(StorageContainer containerKey)
      throws IOException, ContainerAlreadyExistsException;

  /**
   * Return a read-only set view of known key-value containers in a
   * given block pool. The set of containers can change while an
   * iteration is in progress causing some newly added containers to
   * be omitted in an iteration.
   *
   * @param bpid the block pool ID in which to enumerate containers.
   *
   * @return a read only set view of all known key-value containers.
   */
  Set<StorageContainer> getContainerSet(String bpid);

  /**
   * Lookup a key-value container. The provided generation stamp and
   * transactionId must match the latest values known to the dataset.
   *
   * @param containerKey key identifying the container. The id, generation
   *                     stamp, blockpoolId and transactionId fields must
   *                     be initialized.
   *
   * @return a reference to an existing container.
   *
   * @throws StorageContainerNotFoundException if the container was not found.
   * @throws BadGenerationStampException if the provided generation stamp does
   *                                     not match what we have.
   * @throws BadTransactionIdException if the provided transactionId does not
   *                                   match what we have.
   * @throws IOException if the operation failed for any other reason.
   */
  KeyValueContainer lookupContainer(StorageContainer containerKey)
      throws BadGenerationStampException, BadTransactionIdException,
             StorageContainerNotFoundException, IOException;

  /**
   * Delete a storage container. This deletes all key-value pairs in
   * the container. All container resources must be disposed off before
   * calling deleteContainer.
   *
   * The caller must ensure that a reference to the container is not in
   * use before attempting to delete the container.
   *
   * @throws StorageContainerNotFoundException if the container was not found.
   * @throws IOException if the operation failed for any other reason.
   */
  void deleteContainer(StorageContainer container)
      throws StorageContainerNotFoundException, IOException;

  /**
   * Create a blob. Returns a FileChannel that can be used to write data
   * into the blob. BlobIds must be unique across the storage service.
   *
   * @return a Channel for async writes into the blob. Any attempts
   *         to read from the channel will throw an exception.
   *
   * @throws IOException if the operation failed for any reason.
   */
  FileChannel createBlob(String bpid, UUID blobId) throws IOException;

  /**
   * Open a channel to read blob data.
   *
   * @return a Channel for async reads from the blob. Any attempts
   *         to write to the channel will throw an exception.
   *
   * @throws IOException if the operation failed for any reason.
   */
  FileChannel openBlob(String bpid, UUID blobId) throws IOException;

  /**
   * Delete a blob.
   */
  void deleteBlob(String bpid, UUID blobId) throws IOException;

  /**
   * Return a read-only set view of known key-value blobs. The
   * set of blobs can change while an iteration is in progress
   * causing some newly added blobs to be omitted in the iteration.
   *
   * @return a read-only set view of all known blobs.
   */
  Set<UUID> getBlobs(String bpid);
}
