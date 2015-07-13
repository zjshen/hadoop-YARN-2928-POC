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
import org.apache.hadoop.hdfs.server.datanode.dataset.VolumeSpi;
import org.apache.hadoop.storagecontainer.protocol.StorageContainer;

import java.io.IOException;

/**
 * Interface to an internal key-value storage container. Allows updating and
 * querying the contents of the container.
 */
@InterfaceAudience.Private
public interface KeyValueContainer {
  void init(StorageContainer containerKey) throws IOException;

  /**
   * Return the underlying storage volume for this container.
   *
   * @return underlying storage volume for this container.
   */
  VolumeSpi getVolume();

  /**
   * Get a new iterator to the container contents. The iterator must be
   * initialized via {@link KeyValueContainerIterator#seek(byte[])} or
   * {@link KeyValueContainerIterator#seekToFirst()} to be valid.
   *
   * @return an iterator over known key-value pairs in the container.
   *
   * @throws IOException if the operation failed for any reason.
   */
  KeyValueContainerIterator getIterator() throws IOException;

  /**
   * Atomically insert a key-value pair into the container. If the insert is
   * successful then the transaction id of the container is updated.
   *
   * If the key already exists then its value is updated. This can be
   * thought of as an upsert operation.
   *
   * Insertion and transaction id update must be atomic i.e. both or neither.
   *
   * @param txid Transaction id corresponding to the operation.
   * @param key key to insert into the container.
   * @param value value corresponding to the given key.
   *
   * @return reference to this object to allow chaining calls.
   *
   * @throws IOException if the operation failed for any reason.
   */
  KeyValueContainer put(long txid, byte[] key, byte[] value)
      throws IOException;

  /**
   * Retrieve the value associated with the given key.
   *
   * @param key
   *
   * @return value associated with the given key.
   *
   * @throws IOException if the operation failed for any reason.
   */
  byte[] get(byte[] key) throws IOException;

  /**
   * Remove the key and its associated value from the container. If the
   * remove is successful then the transaction id of the container is
   * updated.
   *
   * Removal and transaction id update must be atomic i.e. both or neither.
   *
   * @param txid Transaction id corresponding to the operation.
   * @param key
   *
   * @return reference to this object to allow chaining puts.
   *
   * @throws IOException if the operation failed for any reason.
   */
  KeyValueContainer remove(long txid, byte[] key) throws IOException;

  /**
   * Delete the container if it is empty.
   *
   * @throws IOException if the operation failed for any reason.
   */
  void delete() throws IOException;

  /**
   * Close the container, releasing any in-memory resources associated with
   * the container.
   *
   * @throws IOException if the operation failed for any reason.
   */
  void close() throws IOException;

  /**
   * Retrieve the container ID.
   * @return the container ID.
   */
  long getContainerId();

  /**
   * Return the generation stamp associated with the container.
   *
   * @return generation stamp associated with the container.
   */
  long getGenerationStamp();

  /**
   * Update the generation stamp associated with the container.
   *
   * @param newGenerationStamp
   *
   * @return reference to this object to allow chaining puts.
   *
   * @throws IOException if the operation failed for any reason.
   */
  KeyValueContainer updateGenerationStamp(long newGenerationStamp)
      throws IOException;

  /**
   * Retrieve the transaction id associated with the last successful
   * update to this container.
   *
   * @return transaction id associated with the last successful update.
   */
  long getTransactionId();

  /**
   * Retrieve the key associated with this container. The key is a dummy
   * StorageContainer object.
   *
   * @return container key
   */
  StorageContainer toContainerKey();
}
