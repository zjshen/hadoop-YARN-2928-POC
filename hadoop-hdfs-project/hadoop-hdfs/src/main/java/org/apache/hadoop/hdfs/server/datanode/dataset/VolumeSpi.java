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
package org.apache.hadoop.hdfs.server.datanode.dataset;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;

/**
 * This is an interface for the underlying volume.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface VolumeSpi {
  /**
   * @return the available storage space in bytes.
   */
  long getAvailable() throws IOException;

  /**
   * @return the base path to the volume
   */
  String getBasePath();

  /**
   * @return the StorageUuid of the volume
   */
  String getStorageID();

  /**
   * Returns true if the volume is NOT backed by persistent storage.
   */
  boolean isTransientStorage();

  /**
   * @return a list of block pools.
   */
  String[] getBlockPoolList();

  /**
   * @return the path to the volume
   */
  String getPath(String bpid) throws IOException;

  /**
   * Return the StorageType i.e. media type of this volume.
   * @return
   */
  StorageType getStorageType();

  /**
   * Get the DatasetSpi which this volume is a part of.
   */
  DatasetSpi<? extends VolumeSpi> getDataset();
}
