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

package org.apache.sqoop.connector.hdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NodeBase;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.hdfs.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.hdfs.configuration.LinkConfiguration;
import org.apache.sqoop.error.code.HdfsConnectorError;
import org.apache.sqoop.job.etl.Partition;
import org.apache.sqoop.job.etl.Partitioner;
import org.apache.sqoop.job.etl.PartitionerContext;

/**
 * This class derives mostly from CombineFileInputFormat of Hadoop, i.e.
 * org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat.
 */
public class HdfsPartitioner extends Partitioner<LinkConfiguration, FromJobConfiguration> {

  public static final String SPLIT_MINSIZE_PERNODE =
      "mapreduce.input.fileinputformat.split.minsize.per.node";
  public static final String SPLIT_MINSIZE_PERRACK =
      "mapreduce.input.fileinputformat.split.minsize.per.rack";

  // ability to limit the size of a single split
  private long maxSplitSize = 0;
  private long minSplitSizeNode = 0;
  private long minSplitSizeRack = 0;

  // mapping from a rack name to the set of Nodes in the rack
  private HashMap<String, Set<String>> rackToNodes =
      new HashMap<String, Set<String>>();

  @Override
  public List<Partition> getPartitions(PartitionerContext context,
                                       LinkConfiguration linkConfiguration,
                                       FromJobConfiguration fromJobConfig) {
    Configuration conf = new Configuration();
    HdfsUtils.contextToConfiguration(context.getContext(), conf);

    try {
      long numInputBytes = getInputSize(conf, fromJobConfig.fromJobConfig.inputDirectory);
      maxSplitSize = numInputBytes / context.getMaxPartitions();

      if(numInputBytes % context.getMaxPartitions() != 0 ) {
        maxSplitSize += 1;
       }

      long minSizeNode = 0;
      long minSizeRack = 0;
      long maxSize = 0;

      // the values specified by setxxxSplitSize() takes precedence over the
      // values that might have been specified in the config
      if (minSplitSizeNode != 0) {
        minSizeNode = minSplitSizeNode;
      } else {
        minSizeNode = conf.getLong(SPLIT_MINSIZE_PERNODE, 0);
      }
      if (minSplitSizeRack != 0) {
        minSizeRack = minSplitSizeRack;
      } else {
        minSizeRack = conf.getLong(SPLIT_MINSIZE_PERRACK, 0);
      }
      if (maxSplitSize != 0) {
        maxSize = maxSplitSize;
      } else {
        maxSize = conf.getLong("mapreduce.input.fileinputformat.split.maxsize", 0);
      }
      if (minSizeNode != 0 && maxSize != 0 && minSizeNode > maxSize) {
        throw new IOException("Minimum split size pernode " + minSizeNode +
                              " cannot be larger than maximum split size " +
                              maxSize);
      }
      if (minSizeRack != 0 && maxSize != 0 && minSizeRack > maxSize) {
        throw new IOException("Minimum split size per rack" + minSizeRack +
                              " cannot be larger than maximum split size " +
                              maxSize);
      }
      if (minSizeRack != 0 && minSizeNode > minSizeRack) {
        throw new IOException("Minimum split size per node" + minSizeNode +
                              " cannot be smaller than minimum split " +
                              "size per rack " + minSizeRack);
      }

      // all the files in input set
      String indir = fromJobConfig.fromJobConfig.inputDirectory;
      FileSystem fs = FileSystem.get(conf);

      List<Path> paths = new LinkedList<Path>();
      for(FileStatus status : fs.listStatus(new Path(indir))) {
        if(!status.isDir()) {
          paths.add(status.getPath());
        }
      }

      List<Partition> partitions = new ArrayList<Partition>();
      if (paths.size() == 0) {
        return partitions;
      }

      // create splits for all files that are not in any pool.
      getMoreSplits(conf, paths,
                    maxSize, minSizeNode, minSizeRack, partitions);

      // free up rackToNodes map
      rackToNodes.clear();

      return partitions;

    } catch (IOException e) {
      throw new SqoopException(HdfsConnectorError.GENERIC_HDFS_CONNECTOR_0000, e);
    }
  }

  //TODO: Perhaps get the FS from link configuration so we can support remote HDFS
  private long getInputSize(Configuration conf, String indir) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] files = fs.listStatus(new Path(indir));
    long count = 0;
    for (FileStatus file : files) {
      count += file.getLen();
    }
    return count;
  }

  /**
   * Return all the splits in the specified set of paths
   */
  private void getMoreSplits(Configuration conf, List<Path> paths,
      long maxSize, long minSizeNode, long minSizeRack,
      List<Partition> partitions) throws IOException {

    // all blocks for all the files in input set
    OneFileInfo[] files;

    // mapping from a rack name to the list of blocks it has
    HashMap<String, List<OneBlockInfo>> rackToBlocks =
                              new HashMap<String, List<OneBlockInfo>>();

    // mapping from a block to the nodes on which it has replicas
    HashMap<OneBlockInfo, String[]> blockToNodes =
                              new HashMap<OneBlockInfo, String[]>();

    // mapping from a node to the list of blocks that it contains
    HashMap<String, List<OneBlockInfo>> nodeToBlocks =
                              new HashMap<String, List<OneBlockInfo>>();

    files = new OneFileInfo[paths.size()];
    if (paths.size() == 0) {
      return;
    }

    // populate all the blocks for all files
    for (int i = 0; i < paths.size(); i++) {
      files[i] = new OneFileInfo(paths.get(i), conf, isSplitable(conf, paths.get(i)),
                                 rackToBlocks, blockToNodes, nodeToBlocks,
                                 rackToNodes, maxSize);
    }

    ArrayList<OneBlockInfo> validBlocks = new ArrayList<OneBlockInfo>();
    Set<String> nodes = new HashSet<String>();
    long curSplitSize = 0;

    // process all nodes and create splits that are local
    // to a node.
    for (Iterator<Map.Entry<String,
         List<OneBlockInfo>>> iter = nodeToBlocks.entrySet().iterator();
         iter.hasNext();) {

      Map.Entry<String, List<OneBlockInfo>> one = iter.next();
      nodes.add(one.getKey());
      List<OneBlockInfo> blocksInNode = one.getValue();

      // for each block, copy it into validBlocks. Delete it from
      // blockToNodes so that the same block does not appear in
      // two different splits.
      for (OneBlockInfo oneblock : blocksInNode) {
        if (blockToNodes.containsKey(oneblock)) {
          validBlocks.add(oneblock);
          blockToNodes.remove(oneblock);
          curSplitSize += oneblock.length;

          // if the accumulated split size exceeds the maximum, then
          // create this split.
          if (maxSize != 0 && curSplitSize >= maxSize) {
            // create an input split and add it to the splits array
            addCreatedSplit(partitions, nodes, validBlocks);
            curSplitSize = 0;
            validBlocks.clear();
          }
        }
      }
      // if there were any blocks left over and their combined size is
      // larger than minSplitNode, then combine them into one split.
      // Otherwise add them back to the unprocessed pool. It is likely
      // that they will be combined with other blocks from the
      // same rack later on.
      if (minSizeNode != 0 && curSplitSize >= minSizeNode) {
        // create an input split and add it to the splits array
        addCreatedSplit(partitions, nodes, validBlocks);
      } else {
        for (OneBlockInfo oneblock : validBlocks) {
          blockToNodes.put(oneblock, oneblock.hosts);
        }
      }
      validBlocks.clear();
      nodes.clear();
      curSplitSize = 0;
    }

    // if blocks in a rack are below the specified minimum size, then keep them
    // in 'overflow'. After the processing of all racks is complete, these
    // overflow blocks will be combined into splits.
    ArrayList<OneBlockInfo> overflowBlocks = new ArrayList<OneBlockInfo>();
    Set<String> racks = new HashSet<String>();

    // Process all racks over and over again until there is no more work to do.
    while (blockToNodes.size() > 0) {

      // Create one split for this rack before moving over to the next rack.
      // Come back to this rack after creating a single split for each of the
      // remaining racks.
      // Process one rack location at a time, Combine all possible blocks that
      // reside on this rack as one split. (constrained by minimum and maximum
      // split size).

      // iterate over all racks
      for (Iterator<Map.Entry<String, List<OneBlockInfo>>> iter =
           rackToBlocks.entrySet().iterator(); iter.hasNext();) {

        Map.Entry<String, List<OneBlockInfo>> one = iter.next();
        racks.add(one.getKey());
        List<OneBlockInfo> blocks = one.getValue();

        // for each block, copy it into validBlocks. Delete it from
        // blockToNodes so that the same block does not appear in
        // two different splits.
        boolean createdSplit = false;
        for (OneBlockInfo oneblock : blocks) {
          if (blockToNodes.containsKey(oneblock)) {
            validBlocks.add(oneblock);
            blockToNodes.remove(oneblock);
            curSplitSize += oneblock.length;

            // if the accumulated split size exceeds the maximum, then
            // create this split.
            if (maxSize != 0 && curSplitSize >= maxSize) {
              // create an input split and add it to the splits array
              addCreatedSplit(partitions, getHosts(racks), validBlocks);
              createdSplit = true;
              break;
            }
          }
        }

        // if we created a split, then just go to the next rack
        if (createdSplit) {
          curSplitSize = 0;
          validBlocks.clear();
          racks.clear();
          continue;
        }

        if (!validBlocks.isEmpty()) {
          if (minSizeRack != 0 && curSplitSize >= minSizeRack) {
            // if there is a minimum size specified, then create a single split
            // otherwise, store these blocks into overflow data structure
            addCreatedSplit(partitions, getHosts(racks), validBlocks);
          } else {
            // There were a few blocks in this rack that
            // remained to be processed. Keep them in 'overflow' block list.
            // These will be combined later.
            overflowBlocks.addAll(validBlocks);
          }
        }
        curSplitSize = 0;
        validBlocks.clear();
        racks.clear();
      }
    }

    assert blockToNodes.isEmpty();
    assert curSplitSize == 0;
    assert validBlocks.isEmpty();
    assert racks.isEmpty();

    // Process all overflow blocks
    for (OneBlockInfo oneblock : overflowBlocks) {
      validBlocks.add(oneblock);
      curSplitSize += oneblock.length;

      // This might cause an exiting rack location to be re-added,
      // but it should be ok.
      for (int i = 0; i < oneblock.racks.length; i++) {
        racks.add(oneblock.racks[i]);
      }

      // if the accumulated split size exceeds the maximum, then
      // create this split.
      if (maxSize != 0 && curSplitSize >= maxSize) {
        // create an input split and add it to the splits array
        addCreatedSplit(partitions, getHosts(racks), validBlocks);
        curSplitSize = 0;
        validBlocks.clear();
        racks.clear();
      }
    }

    // Process any remaining blocks, if any.
    if (!validBlocks.isEmpty()) {
      addCreatedSplit(partitions, getHosts(racks), validBlocks);
    }
  }

  private boolean isSplitable(Configuration conf, Path file) {
    final CompressionCodec codec =
        new CompressionCodecFactory(conf).getCodec(file);

    // This method might be improved for SplittableCompression codec when we
    // drop support for Hadoop 1.0
    return null == codec;

  }

  /**
   * Create a single split from the list of blocks specified in validBlocks
   * Add this new split into list.
   */
  private void addCreatedSplit(List<Partition> partitions,
                               Collection<String> locations,
                               ArrayList<OneBlockInfo> validBlocks) {
    // create an input split
    Path[] files = new Path[validBlocks.size()];
    long[] offsets = new long[validBlocks.size()];
    long[] lengths = new long[validBlocks.size()];
    for (int i = 0; i < validBlocks.size(); i++) {
      files[i] = validBlocks.get(i).onepath;
      offsets[i] = validBlocks.get(i).offset;
      lengths[i] = validBlocks.get(i).length;
    }

     // add this split to the list that is returned
    HdfsPartition partition = new HdfsPartition(
        files, offsets, lengths, locations.toArray(new String[0]));
    partitions.add(partition);
  }

  private Set<String> getHosts(Set<String> racks) {
    Set<String> hosts = new HashSet<String>();
    for (String rack : racks) {
      if (rackToNodes.containsKey(rack)) {
        hosts.addAll(rackToNodes.get(rack));
      }
    }
    return hosts;
  }

  private static void addHostToRack(HashMap<String, Set<String>> rackToNodes,
      String rack, String host) {
    Set<String> hosts = rackToNodes.get(rack);
    if (hosts == null) {
      hosts = new HashSet<String>();
      rackToNodes.put(rack, hosts);
    }
    hosts.add(host);
  }

  /**
   * information about one file from the File System
   */
  private static class OneFileInfo {
    private long fileSize;               // size of the file
    private OneBlockInfo[] blocks;       // all blocks in this file

    OneFileInfo(Path path, Configuration conf,
                boolean isSplitable,
                HashMap<String, List<OneBlockInfo>> rackToBlocks,
                HashMap<OneBlockInfo, String[]> blockToNodes,
                HashMap<String, List<OneBlockInfo>> nodeToBlocks,
                HashMap<String, Set<String>> rackToNodes,
                long maxSize)
                throws IOException {
      this.fileSize = 0;

      // get block locations from file system
      FileSystem fs = path.getFileSystem(conf);
      FileStatus stat = fs.getFileStatus(path);
      BlockLocation[] locations = fs.getFileBlockLocations(stat, 0,
                                                           stat.getLen());
      // create a list of all block and their locations
      if (locations == null) {
        blocks = new OneBlockInfo[0];
      } else {
        if (!isSplitable) {
          // if the file is not splitable, just create the one block with
          // full file length
          blocks = new OneBlockInfo[1];
          fileSize = stat.getLen();
          blocks[0] = new OneBlockInfo(path, 0, fileSize, locations[0]
              .getHosts(), locations[0].getTopologyPaths());
        } else {
          ArrayList<OneBlockInfo> blocksList = new ArrayList<OneBlockInfo>(
              locations.length);
          for (int i = 0; i < locations.length; i++) {
            fileSize += locations[i].getLength();

            // each split can be a maximum of maxSize
            long left = locations[i].getLength();
            long myOffset = locations[i].getOffset();
            long myLength = 0;
            do {
              if (maxSize == 0) {
                myLength = left;
              } else {
                if (left > maxSize && left < 2 * maxSize) {
                  // if remainder is between max and 2*max - then
                  // instead of creating splits of size max, left-max we
                  // create splits of size left/2 and left/2. This is
                  // a heuristic to avoid creating really really small
                  // splits.
                  myLength = left / 2;
                } else {
                  myLength = Math.min(maxSize, left);
                }
              }
              OneBlockInfo oneblock = new OneBlockInfo(path, myOffset,
                  myLength, locations[i].getHosts(), locations[i]
                      .getTopologyPaths());
              left -= myLength;
              myOffset += myLength;

              blocksList.add(oneblock);
            } while (left > 0);
          }
          blocks = blocksList.toArray(new OneBlockInfo[blocksList.size()]);
        }

        for (OneBlockInfo oneblock : blocks) {
          // add this block to the block --> node locations map
          blockToNodes.put(oneblock, oneblock.hosts);

          // For blocks that do not have host/rack information,
          // assign to default  rack.
          String[] racks = null;
          if (oneblock.hosts.length == 0) {
            racks = new String[]{NetworkTopology.DEFAULT_RACK};
          } else {
            racks = oneblock.racks;
          }

          // add this block to the rack --> block map
          for (int j = 0; j < racks.length; j++) {
            String rack = racks[j];
            List<OneBlockInfo> blklist = rackToBlocks.get(rack);
            if (blklist == null) {
              blklist = new ArrayList<OneBlockInfo>();
              rackToBlocks.put(rack, blklist);
            }
            blklist.add(oneblock);
            if (!racks[j].equals(NetworkTopology.DEFAULT_RACK)) {
              // Add this host to rackToNodes map
              addHostToRack(rackToNodes, racks[j], oneblock.hosts[j]);
            }
          }

          // add this block to the node --> block map
          for (int j = 0; j < oneblock.hosts.length; j++) {
            String node = oneblock.hosts[j];
            List<OneBlockInfo> blklist = nodeToBlocks.get(node);
            if (blklist == null) {
              blklist = new ArrayList<OneBlockInfo>();
              nodeToBlocks.put(node, blklist);
            }
            blklist.add(oneblock);
          }
        }
      }
    }

  }

  /**
   * information about one block from the File System
   */
  private static class OneBlockInfo {
    Path onepath;                // name of this file
    long offset;                 // offset in file
    long length;                 // length of this block
    String[] hosts;              // nodes on which this block resides
    String[] racks;              // network topology of hosts

    OneBlockInfo(Path path, long offset, long len,
                 String[] hosts, String[] topologyPaths) {
      this.onepath = path;
      this.offset = offset;
      this.hosts = hosts;
      this.length = len;
      assert (hosts.length == topologyPaths.length ||
              topologyPaths.length == 0);

      // if the file system does not have any rack information, then
      // use dummy rack location.
      if (topologyPaths.length == 0) {
        topologyPaths = new String[hosts.length];
        for (int i = 0; i < topologyPaths.length; i++) {
          topologyPaths[i] = (new NodeBase(hosts[i],
                              NetworkTopology.DEFAULT_RACK)).toString();
        }
      }

      // The topology paths have the host name included as the last
      // component. Strip it.
      this.racks = new String[topologyPaths.length];
      for (int i = 0; i < topologyPaths.length; i++) {
        this.racks[i] = (new NodeBase(topologyPaths[i])).getNetworkLocation();
      }
    }
  }

}
