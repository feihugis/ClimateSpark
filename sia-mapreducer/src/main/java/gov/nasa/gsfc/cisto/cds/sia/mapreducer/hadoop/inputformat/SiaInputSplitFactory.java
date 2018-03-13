package gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.inputformat;

import gov.nasa.gsfc.cisto.cds.sia.core.io.SiaChunk;
import gov.nasa.gsfc.cisto.cds.sia.core.variableentities.Merra2VariableEntity;
import gov.nasa.gsfc.cisto.cds.sia.core.variableentities.SiaVariableEntity;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by Fei Hu on 9/12/16.
 */
public class SiaInputSplitFactory {

  static public List<SiaChunk> convertSIAVarEntityListToSIAChunkList (List<SiaVariableEntity> siaVariableEntityList,
                                                                      int[] shape, String[] dims, String varName, String filePathPrifix, String filePathSuffix) {
    List<SiaChunk> siaChunkList = new ArrayList<SiaChunk>();
    for (SiaVariableEntity entity : siaVariableEntityList) {
      siaChunkList.add(convertSIAVarEntityToSIAChunk(entity, shape, dims, varName, filePathPrifix, filePathSuffix));
    }
    return null;
  }

  static public List<SiaChunk> convertSIAVarEntityListToSIAChunkList (List<SiaVariableEntity> siaVariableEntityList,
                                                                      int[] shape, String[] dims, String varName, List<FileStatus> fileStatusList) {
    List<SiaChunk> siaChunkList = new ArrayList<SiaChunk>();
    for (SiaVariableEntity entity : siaVariableEntityList) {
      siaChunkList.add(convertSIAVarEntityToSIAChunk(entity, shape, dims, varName, fileStatusList));
    }
    return null;
  }

  static public SiaChunk convertSIAVarEntityToSIAChunk (SiaVariableEntity siaVariableEntity, int[] shape, String[] dims, String varName, List<FileStatus> fileStatusList) {
    SiaChunk siaChunk = null;

    //TODO: add merra-1 here

    if (siaVariableEntity.getClass().getName().equals(Merra2VariableEntity.class.getName())) {
      Merra2VariableEntity merra2VariableEntity = (Merra2VariableEntity) siaVariableEntity;
      String[] cornerTmp = merra2VariableEntity.getCorner().split(",");
      int[] corner = new int[cornerTmp.length];
      for (int i=0; i<corner.length; i++) {
        corner[i] = Integer.parseInt(cornerTmp[i]);
      }

      long filePos = merra2VariableEntity.getByteOffset();
      long byteSize = merra2VariableEntity.getByteLength();
      int filterMask = merra2VariableEntity.getCompressionCode();
      String[] hosts = merra2VariableEntity.getBlockHosts().split(",");
      String dataType = "float";
      int time = merra2VariableEntity.getTemporalComponent();
      String geometryInfo = merra2VariableEntity.getGeometryID()+"";

      String filePath = null;
      for (FileStatus fileStatus : fileStatusList) {
        if (fileStatus.getPath().toString().contains(time+"")) {
          filePath = fileStatus.getPath().toString();
          siaChunk = new SiaChunk(corner, shape, dims, filePos, byteSize, filterMask, hosts, dataType, varName, filePath, time, geometryInfo);
          break;
        }
      }
    }

    return siaChunk;
  }

  public static SiaChunk convertSIAVarEntityToSIAChunk (SiaVariableEntity siaVariableEntity, int[] shape, String[] dims, String varName, String filePathPrifix, String filePathSuffix) {
    SiaChunk siaChunk = null;

    //TODO: add merra-1 here
    if (siaVariableEntity.getClass().getName().equals(Merra2VariableEntity.class.getName())) {
      Merra2VariableEntity merra2VariableEntity = (Merra2VariableEntity) siaVariableEntity;
      String[] cornerTmp = merra2VariableEntity.getCorner().split(",");
      int[] corner = new int[cornerTmp.length];
      for (int i=0; i<corner.length; i++) {
        corner[i] = Integer.parseInt(cornerTmp[i]);
      }

      long filePos = merra2VariableEntity.getByteOffset();
      long byteSize = merra2VariableEntity.getByteLength();
      int filterMask = merra2VariableEntity.getCompressionCode();
      String[] hosts = merra2VariableEntity.getBlockHosts().split(",");
      String dataType = "float";
      int time = merra2VariableEntity.getTemporalComponent();
      String geometryInfo = merra2VariableEntity.getGeometryID()+"";
      String filePath = filePathPrifix + time + filePathSuffix;

      siaChunk = new SiaChunk(corner, shape, dims, filePos, byteSize, filterMask, hosts, dataType, varName, filePath, time, geometryInfo);
    }

    return siaChunk;
  }

  public static List<SiaInputSplit> genSIAInputSplitByHosts(List<SiaChunk> inputDataChunkList) {
    List<SiaInputSplit> inputSplitList = new ArrayList<SiaInputSplit>();
    List<SiaChunk> chunkList = new ArrayList<SiaChunk>();
    inputDataChunkList.add(null);
    for(int i=0; i<inputDataChunkList.size()-1; i++) {
      chunkList.add(inputDataChunkList.get(i));
      if (isShareHosts(inputDataChunkList.get(i), inputDataChunkList.get(i+1))
          && inputDataChunkList.get(i).getFilePath().equals(inputDataChunkList.get(i+1).getFilePath())) {
        continue;
      } else {
        inputSplitList.add(new SiaInputSplit(chunkList));
        chunkList = new ArrayList<SiaChunk>();
      }
    }

    return inputSplitList;
  }

  public static List<GroupedSIAInputSplit> genGroupedSIAInputSplit(List<SiaInputSplit> siaInputSplitList, int taskNumPerNode)
      throws IOException, InterruptedException {
    List<GroupedSIAInputSplit> groupedSIAInputSplitList = new ArrayList<GroupedSIAInputSplit>();
    HashMap<String, List<SiaInputSplit>> hostToSiaInputSplitMap = new HashMap<String, List<SiaInputSplit>>();
    Iterator<SiaInputSplit> iterator = siaInputSplitList.iterator();

    while (iterator.hasNext()) {
      SiaInputSplit siaInputSplit = iterator.next();
      String curHost = "";
      for (String host : siaInputSplit.getLocations()) {
        if (curHost.isEmpty() || !hostToSiaInputSplitMap.containsKey(host)) {
          curHost = host;
        } else if (hostToSiaInputSplitMap.containsKey(host)) {
          curHost = hostToSiaInputSplitMap.get(curHost).size() > hostToSiaInputSplitMap.get(host).size()? host : curHost;
        }
      }

      List<SiaInputSplit> value = hostToSiaInputSplitMap.getOrDefault(curHost, new ArrayList<SiaInputSplit>());
      value.add(siaInputSplit);
      hostToSiaInputSplitMap.put(curHost, value);
    }

    Set<Map.Entry<String, List<SiaInputSplit>>> entrySet = hostToSiaInputSplitMap.entrySet();

    for (Map.Entry<String, List<SiaInputSplit>> entry : entrySet) {
      String host = entry.getKey();
      List<SiaInputSplit> siaInputSplitsPerHost = entry.getValue();

      int insputSplitNumPerGroup = siaInputSplitsPerHost.size() / taskNumPerNode + 1;
      int start = 0, end = start + insputSplitNumPerGroup;
      while (start < siaInputSplitsPerHost.size()) {
        List<SiaInputSplit> subSiaInputSplitList = siaInputSplitsPerHost
            .subList(start, Math.min(end, siaInputSplitsPerHost.size()));

        GroupedSIAInputSplit groupedSIAInputSplit = new GroupedSIAInputSplit(subSiaInputSplitList,
                                                                             new String[]{host});
        groupedSIAInputSplitList.add(groupedSIAInputSplit);
        start += insputSplitNumPerGroup;
        end += insputSplitNumPerGroup;
      }
    }

    return groupedSIAInputSplitList;
  }

  public static boolean isShareHosts(SiaChunk chunk1, SiaChunk chunk2) {
    if (chunk2 == null) {
      return false;
    }
    String[] hosts1 = chunk1.getHosts();
    String[] hosts2 = chunk2.getHosts();

    for (int i=0; i<hosts1.length; i++) {
      for (int j = 0; j < hosts2.length; j++) {
        if (hosts1[i].equals(hosts2[j])) {
          return true;
        }
      }
    }
    return false;
  }


  /**
   * Get the intersection between two input array boundary
   * @param orgCorner
   * @param orgShape
   * @param queryCorner
   * @param queryShape
   * @param targetCorner
   * @param targetShape
   * @return
   */
  public static boolean isIntersected(int[] orgCorner, int[] orgShape, int[] queryCorner, int[] queryShape, int[] targetCorner, int[] targetShape) {
    if (orgCorner.length != queryCorner.length) {
      return  false;
    }

    int[] orgEnd = new int[orgCorner.length], queryEnd = new int[queryCorner.length];

    for (int i=0; i<orgCorner.length; i++) {
      orgEnd[i] = orgCorner[i] + orgShape[i] - 1;
      queryEnd[i] = queryCorner[i] + queryShape[i] - 1;
      if ((orgCorner[i]<queryCorner[i]&&orgEnd[i]<queryCorner[i]) || (orgCorner[i]>queryEnd[i]&&orgEnd[i]>queryEnd[i])) {
        return false;
      }
    }

    int[] targetEnd = new int[orgCorner.length];
    for (int i=0; i<orgCorner.length; i++) {
      targetCorner[i] = Math.max(orgCorner[i], queryCorner[i]);
      targetEnd[i] = Math.min(orgEnd[i], queryEnd[i]);
      targetShape[i] = targetEnd[i] - targetCorner[i] + 1;
    }

    return true;
  }

  public static boolean isIntersected(int[] orgCorner, int[] orgShape, int[] queryCorner, int[] queryShape) {
    if (orgCorner.length != queryCorner.length) {
      return  false;
    }

    int[] orgEnd = new int[orgCorner.length], queryEnd = new int[queryCorner.length];

    for (int i=0; i<orgCorner.length; i++) {
      orgEnd[i] = orgCorner[i] + orgShape[i] - 1;
      queryEnd[i] = queryCorner[i] + queryShape[i] - 1;
      if ((orgCorner[i] < queryCorner[i] && orgEnd[i] < queryCorner[i])
          || (orgCorner[i] > queryEnd[i] && orgEnd[i] > queryEnd[i])) {
        return false;
      }
    }

    return true;
  }

  public static int[] stringToIntArray(String array, String split) {
    String[] arrays = array.split(split);
    int[] intArray = new int[arrays.length];
    for (int i = 0; i < arrays.length; i++) {
      intArray[i] = Integer.parseInt(arrays[i]);
    }
    return intArray;
  }

  public static int[] integerListToIntArray(List<Integer> input) {
    int[] chunkShape = new int[input.size()];
    for (int i = 0; i < chunkShape.length; i++) {
      chunkShape[i] = input.get(i);
    }
    return chunkShape;
  }

  public static String[] stringListToStringArray(List<String> input) {
    return input.toArray(new String[input.size()]);
  }


}
