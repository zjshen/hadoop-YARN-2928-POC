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


import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetFactory;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This is a service provider interface for the underlying storage that
 * stores replicas for a data node.
 * The default implementation stores replicas on local drives.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface DatasetSpi<V extends VolumeSpi> {
  /**
   * A factory for creating {@link FsDatasetSpi} objects.
   */
  abstract class Factory {
    /**
     * @return the configured factory.
     */
    public static Factory getFactory(Configuration conf, NodeType nodeType) {
      switch(nodeType) {
      case NAME_NODE:
        @SuppressWarnings("rawtypes")
        final Class<? extends Factory> clazz = conf.getClass(
            DFSConfigKeys.DFS_DATANODE_FSDATASET_FACTORY_KEY,
            FsDatasetFactory.class,
            Factory.class);
        return ReflectionUtils.newInstance(clazz, conf);
      default:
        throw new IllegalArgumentException("Unsupported NODE_TYPE " + nodeType);
      }
    }

    /**
     * Create a new dataset object for a specific service type.
     * The caller must perform synchronization, if required.
     */
    public abstract DatasetSpi<? extends VolumeSpi> newInstance(
        DataNode datanode, DataStorage storage, Configuration conf)
        throws IOException;

    /** Does the factory create simulated objects? */
    public boolean isSimulated() {
      return false;
    }
  }

  /**
   * @return the volume that contains a replica of the block.
   */
  V getVolume(ExtendedBlock b);

  /**
   * Does the dataset contain the block?
   */
  boolean contains(ExtendedBlock block);


  /**
   * Add a new volume to the Dataset.<p/>
   *
   * If the Dataset supports block scanning, this function registers
   * the new volume with the block scanner.
   *
   * @param location      The storage location for the new volume.
   * @param nsInfos       Namespace information for the new volume.
   */
  void addVolume(
      final StorageLocation location,
      final Storage.StorageDirectory sd,
      final List<NamespaceInfo> nsInfos) throws IOException;

  /**
   * Record a failure to bring up a volume. Primarily for reporting
   * purposes.
   *
   * @param location StorageLocation corresponding to the failed volume.
   */
  void recordFailedVolume(final StorageLocation location);

  /**
   * Removes a collection of volumes from FsDataset.
   *
   * If the FSDataset supports block scanning, this function removes
   * the volumes from the block scanner.
   *
   * @param volumes  The paths of the volumes to be removed.
   * @param clearFailure set true to clear the failure information about the
   *                     volumes.
   */
  void removeVolumes(Set<File> volumes, boolean clearFailure);

  /** @return a storage with the given storage ID */
  DatanodeStorage getStorage(final String storageUuid);

  /** @return one or more storage reports for attached volumes. */
  StorageReport[] getStorageReports(String bpid)
      throws IOException;

  /**
   * Returns one block report per volume.
   * @param bpid Block Pool Id
   * @return - a map of DatanodeStorage to block report for the volume.
   */
  Map<DatanodeStorage, BlockListAsLongs> getBlockReports(String bpid);

  /**
   * Invalidates the specified blocks
   * @param bpid Block pool Id
   * @param invalidBlks - the blocks to be invalidated
   * @throws IOException
   */
  void invalidate(String bpid, Block[] invalidBlks) throws IOException;

  /**
   * Returns info about volume failures.
   *
   * @return info about volume failures, possibly null
   */
  VolumeFailureSummary getVolumeFailureSummary();

  /**
   * Check if all the data directories are healthy
   * @return A set of unhealthy data directories.
   */
  Set<File> checkDataDir();

  /**
   * Shutdown the Dataset
   */
  void shutdown();

  /**
   * add new block pool ID
   * @param bpid Block pool Id
   * @param conf Configuration
   */
  void addBlockPool(String bpid, Configuration conf) throws IOException;

  /**
   * Shutdown and remove the block pool from underlying storage.
   * @param bpid Block pool Id to be removed
   */
  void shutdownBlockPool(String bpid);

  /**
   * Checks how many valid storage volumes there are in the DataNode.
   * @return true if more than the minimum number of valid volumes are left
   * in the FSDataSet.
   */
  boolean hasEnoughResource();

  /**
   * Does the dataset support caching blocks?
   *
   * @return
   */
  boolean isCachingSupported();

  /**
   * Caches the specified blocks
   * @param bpid Block pool id
   * @param blockIds - block ids to cache
   */
  void cache(String bpid, long[] blockIds);

  /**
   * Uncaches the specified blocks
   * @param bpid Block pool id
   * @param blockIds - blocks ids to uncache
   */
  void uncache(String bpid, long[] blockIds);


  /**
   * Returns the cache report - the full list of cached block IDs of a
   * block pool.
   * @param   bpid Block Pool Id
   * @return  the cache report - the full list of cached block IDs.
   */
  List<Long> getCacheReport(String bpid);

  /**
   * Enable 'trash' for the given dataset. When trash is enabled, files are
   * moved to a separate trash directory instead of being deleted immediately.
   * This can be useful for example during rolling upgrades.
   */
  void enableTrash(String bpid);

  /**
   * Restore trash
   */
  void clearTrash(String bpid);

  /**
   * @return true when trash is enabled
   */
  boolean trashEnabled(String bpid);

  /**
   * Create a marker file indicating that a rolling upgrade is in progress.
   */
  void setRollingUpgradeMarker(String bpid) throws IOException;

  /**
   * Delete the rolling upgrade marker file if it exists.
   * @param bpid
   */
  void clearRollingUpgradeMarker(String bpid) throws IOException;
}
