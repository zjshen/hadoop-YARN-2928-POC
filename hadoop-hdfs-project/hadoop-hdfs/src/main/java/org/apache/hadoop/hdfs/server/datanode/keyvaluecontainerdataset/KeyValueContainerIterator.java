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

import java.io.IOException;
import java.util.NoSuchElementException;


/**
 * A lexical iterator over a storage container of key-value pairs. The
 * iterator must be positioned using {@link #seek(byte[])} or
 * {@link #seekToFirst()} before the first use.
 *
 * Each Iterator *must* be disposed off by invoking {@link #dispose()} when
 * done using it else the implementation may leak resources.
 */
@InterfaceAudience.Private
public interface KeyValueContainerIterator {

  /**
   * Position the iterator at the beginning of the container.
   *
   * @throws IOException if the operation failed for any reason.
   */
  void seekToFirst() throws IOException;

  /**
   * Positions the iterator at or just before the specified key.
   * If the key exists then a subsequent {@link #nextKey()} call
   * must return the same key.
   *
   * @throws IOException if the operation failed for any reason.
   */
  void seek(byte[] key) throws IOException;

  /**
   * Release the resources associated with this iterator.
   *
   * @throws IOException if the operation failed for any reason.
   */
  void dispose() throws IOException;

  /**
   * Return true if there is an element beyond the current key.
   *
   * @return true if there is an element beyond the current key.
   */
  boolean hasNext();

  /**
   * Advance the iterator by one element.
   *
   * @throws NoSuchElementException if there are no more keys in the
   *         container.
   */
  void next();

  /**
   * Retrieve the key at the iterator position.
   * Retrieve the next key in lexical order if the iterator was positioned
   * at a non-existent key via {@link #seek(byte[])}.
   *
   * @return byte representation of the key corresponding to the next
   *         key-value pair in the container.
   *
   * @throws NoSuchElementException if there are no more keys in the
   *         container.
   */
  byte[] nextKey();

  /**
   * Retrieve the value associated with the next key in the container.
   *
   * @return byte representation of the value corresponding to the next
   *         key-value pair in the container.
   *
   * @throws NoSuchElementException if there are no more keys in the
   *         container.
   */
  byte[] nextValue();
}
