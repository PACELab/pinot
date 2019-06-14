/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.server.api.resources;

import com.google.common.collect.ImmutableList;
import com.linkedin.pinot.common.restlet.resources.ServerPerfMetrics;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.data.manager.offline.InstanceDataManager;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.server.starter.ServerInstance;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * API to provide server performance metrics, for example all hosted segments count and storage size
 */
@Api(tags = "ServerPerfResource")
@Path("/")

public class ServerPerfResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerPerfResource.class);
  private final String TableCPULoadConfigFilePath = "SegmentAssignmentResource/TableCPULoadMetric.properties";
  private  final  String EASYLoadConfigFilePath = "SegmentAssignmentResource/EASYLoadMetric_v2.properties";

  @Inject
  ServerInstance serverInstance;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/ServerPerfMetrics/SegmentInfo")
  @ApiOperation(value = "Show all hosted segments count and storage size", notes = "Storage size and count of all segments hosted by a Pinot Server")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error")})
  public ServerPerfMetrics getSegmentInfo() throws WebApplicationException, IOException {
    InstanceDataManager dataManager = (InstanceDataManager) serverInstance.getInstanceDataManager();
    if (dataManager == null) {
      throw new WebApplicationException("Invalid server initialization", Response.Status.INTERNAL_SERVER_ERROR);
    }


    /*
    ClassLoader classLoader = ServerPerfResource.class.getClassLoader();
    String tableCPULoadModelPath = getFileFromResourceUrl(classLoader.getResource(TableCPULoadConfigFilePath));
    List<String> CPULoadModels = FileUtils.readLines(new File(tableCPULoadModelPath));


    Map<String, CPULoadFormulation> tableCPULoadFormulation = new HashMap<>();
    for (int i = 1; i < CPULoadModels.size(); i++) {
      String[] tableLoadModel = CPULoadModels.get(i).split(",");
      String tableName = tableLoadModel[0];
      double avgDocCount = Double.parseDouble(tableLoadModel[1]);
      double cpuModelA = Double.parseDouble(tableLoadModel[2]);
      double cpuModelB =  Double.parseDouble(tableLoadModel[3]);
      double cpuModelAlpha = Double.parseDouble(tableLoadModel[4]);
      double lifeTimeInDay =  Double.parseDouble(tableLoadModel[5]);
      double timeScale =  Double.parseDouble(tableLoadModel[6]);
      double docScannedModelP1 = Double.parseDouble(tableLoadModel[7]);
      double docScannedModelP2 = Double.parseDouble(tableLoadModel[8]);
      double docScannedModelP3 = Double.parseDouble(tableLoadModel[9]);;

      tableCPULoadFormulation.put(tableName, new CPULoadFormulation(avgDocCount,cpuModelA,cpuModelB,cpuModelAlpha,lifeTimeInDay,timeScale,docScannedModelP1,docScannedModelP2,docScannedModelP3));
    }
    */

    ClassLoader classLoader = ServerPerfResource.class.getClassLoader();
    String tableCPULoadModelPath = getFileFromResourceUrl(classLoader.getResource(EASYLoadConfigFilePath));
    List<String> CPULoadModels = FileUtils.readLines(new File(tableCPULoadModelPath));


    Map<String, EASYLoadFormulation> tableCPULoadFormulation = new HashMap<>();
    for (int i = 1; i < CPULoadModels.size(); i++) {
      String[] tableLoadModel = CPULoadModels.get(i).split(",");
      String tableName = tableLoadModel[0];
      double lifeTimeInDay = Double.parseDouble(tableLoadModel[1]);
      double timeScale =  Double.parseDouble(tableLoadModel[2]);
      double C0 = Double.parseDouble(tableLoadModel[3]);
      double Beta0 = Double.parseDouble(tableLoadModel[4]);
      double C1 = Double.parseDouble(tableLoadModel[5]);
      double Beta1 = Double.parseDouble(tableLoadModel[6]);
      double C2 = Double.parseDouble(tableLoadModel[7]);
      double Beta2 = Double.parseDouble(tableLoadModel[8]);
      double C3 = Double.parseDouble(tableLoadModel[9]);
      double Beta3 = Double.parseDouble(tableLoadModel[10]);

      tableCPULoadFormulation.put(tableName, new EASYLoadFormulation(lifeTimeInDay,timeScale,C0,Beta0,C1,Beta1,C2,Beta2,C3,Beta3));
    }



    ServerPerfMetrics serverPerfMetrics = new ServerPerfMetrics();
    Collection <TableDataManager> tableDataManagers = dataManager.getTableDataManagers();
    int tableIndex = 0;

    for (TableDataManager tableDataManager : tableDataManagers) {
      serverPerfMetrics.tableList.add(tableDataManager.getTableName());
      serverPerfMetrics.segmentTimeInfo.add(new ArrayList <Long>());

      ImmutableList <SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
      try {
        serverPerfMetrics.segmentCount += segmentDataManagers.size();
        for (SegmentDataManager segmentDataManager : segmentDataManagers) {
          IndexSegment segment = segmentDataManager.getSegment();
          serverPerfMetrics.segmentDiskSizeInBytes += segment.getDiskSizeBytes();

          serverPerfMetrics.segmentsHitCount += segment.getSegmentHitCount();
          // TODO Robin: reset the statistics after fetching from server.
          //segment.resetSegmentHitCount();
          //serverPerfMetrics.segmentList.add(segment.getSegmentMetadata());

          // LOGGER.info("adding segment " + segment.getSegmentName() + " to the list in server side! st: " + segment.getSegmentMetadata().getStartTime() + " et: " + segment.getSegmentMetadata().getEndTime());
          String tableName= tableDataManager.getTableName();
          if(!tableCPULoadFormulation.containsKey(tableName))
          {
            LOGGER.debug("Table {} does not have an entry in {}", tableName, TableCPULoadConfigFilePath);
            continue;
          }
          //EASY Paper: 1519948890

          double segmentLoad = tableCPULoadFormulation.get(tableName).computeCPULoad(segment.getSegmentMetadata(),1556668800);
          serverPerfMetrics.segmentCPULoad += segmentLoad;
          //LOGGER.info("SegmentLoadIsComputed: Time:{}, TableName:{}, SegmentName{}, SegmentLoad:{}", System.currentTimeMillis(), tableDataManager.getTableName(), segment.getSegmentMetadata().getName(), segmentLoad);

          serverPerfMetrics.segmentTimeInfo.get(tableIndex).add(segment.getSegmentMetadata().getStartTime());
          serverPerfMetrics.segmentTimeInfo.get(tableIndex).add(segment.getSegmentMetadata().getEndTime());
        }

        tableIndex++;

      } finally {
        // we could release segmentDataManagers as we iterate in the loop above
        // but this is cleaner with clear semantics of usage. Also, above loop
        // executes fast so duration of holding segments is not a concern
        for (SegmentDataManager segmentDataManager : segmentDataManagers) {
          tableDataManager.releaseSegment(segmentDataManager);
        }
      }
    }

    LOGGER.info("TotalSegmentLoadForServer: Time:{}, SegmentLoad:{}", System.currentTimeMillis(), serverPerfMetrics.segmentCPULoad);
    return serverPerfMetrics;
  }


  private static String getFileFromResourceUrl(@Nonnull URL resourceUrl) {
    // For maven cross package use case, we need to extract the resource from jar to a temporary directory.
    String resourceUrlStr = resourceUrl.toString();
    if (resourceUrlStr.contains("jar!")) {
      try {
        String extension = resourceUrlStr.substring(resourceUrlStr.lastIndexOf('.'));
        File tempFile = File.createTempFile("pinot-cpu-load-model-temp", extension);
        String tempFilePath = tempFile.getAbsolutePath();
        //LOGGER.info("Extracting from " + resourceUrlStr + " to " + tempFilePath);
        FileUtils.copyURLToFile(resourceUrl, tempFile);
        return tempFilePath;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      return resourceUrl.getFile();
    }
  }



  class EASYLoadFormulation
  {
    private double _lifeTimeInDay=0;
    private double _timeScale=0;
    private  double _C0=0;
    private  double _Beta0=0;
    private  double _C1=0;
    private  double _Beta1=0;
    private  double _C2=0;
    private  double _Beta2=0;
    private  double _C3=0;
    private  double _Beta3=0;

    public EASYLoadFormulation(double lifeTimeInDay, double timeScale, double C0, double Beta0, double C1, double Beta1, double C2, double Beta2, double C3, double Beta3)
    {
      _lifeTimeInDay = lifeTimeInDay;
      _timeScale = timeScale;
      _C0 = C0;
      _Beta0 = Beta0;
      _C1 = C1;
      _Beta1 = Beta1;
      _C2 = C2;
      _Beta2 = Beta2;
      _C3 = C3;
      _Beta3 = Beta3;
    }

    public double computeCPULoad(SegmentMetadata segmentMetadata, long maxTime) {

      //LOGGER.info("ComputingLoadFor: {}, {}, {}, {}, {}, {}, {}, {}", _avgDocCount, _cpuModelA,_cpuModelB,_cpuModelAlpha,_lifeTimeInDay,_timeScale,_docScannedModelP1,_docScannedModelP2,_docScannedModelP3);

      double lifeTimeInScaleSeconds = (_lifeTimeInDay *24*3600)/_timeScale;

      double segmentMiddleTime = (segmentMetadata.getStartTime() + segmentMetadata.getEndTime()) / 2;
      double segmentAgeInScaleSeconds = (maxTime - segmentMiddleTime)/_timeScale;

      if(segmentAgeInScaleSeconds > lifeTimeInScaleSeconds) {
        return 0;
      }
      double t0= _C0 * ( Math.pow(lifeTimeInScaleSeconds,_Beta0) - Math.pow(segmentAgeInScaleSeconds,_Beta0) );
      double t1= _C1 * ( Math.pow(lifeTimeInScaleSeconds,_Beta1) - Math.pow(segmentAgeInScaleSeconds,_Beta1) );
      double t2= _C2 * ( Math.pow(lifeTimeInScaleSeconds,_Beta2) - Math.pow(segmentAgeInScaleSeconds,_Beta2) );
      double t3= _C3 * ( Math.pow(lifeTimeInScaleSeconds,_Beta3) - Math.pow(segmentAgeInScaleSeconds,_Beta3) );

      double segmentCost = t0 + t1 + t2 + t3;
      segmentCost *= segmentMetadata.getTotalDocs();

      //LOGGER.info("EASYParameters: td: {}, lf:{}, sa:{}, c1:{}, beta1:{}, c2:{}, beta2:{}, t1:{}, t2:{}, Load:{}",segmentMetadata.getTotalDocs(), lifeTimeInScaleSeconds,segmentAgeInScaleSeconds,_C1,_Beta1,_C2,_Beta2, t1, t2, segmentCost);
      //LOGGER.info("DifferentLevelComputation: {}, {}, {}, {}, {}, {}, {}", tmp1,tmp2,tmp3,tmp4,tmp5,perDocCost,segmentCost);
      if(segmentCost < 0)
        segmentCost = 0;
      return segmentCost;
      //return 0;
    }
  }

  class CPULoadFormulation {

    //CPU Load for a segment
    //(SegmentDocCount/avgDocCount)*a*e^(-bt)

    private double _avgDocCount = 0;
    private double _cpuModelA = 0;
    private double _cpuModelB = 0;
    private double _cpuModelAlpha = 0;
    private double _lifeTimeInDay =0;
    private double _timeScale=0;
    private double _docScannedModelP1 = 0;
    private double _docScannedModelP2 = 0;
    private double _docScannedModelP3 = 0;

    public CPULoadFormulation(double avgDocCount, double cpuModelA, double cpuModelAlpha, double cpuModelB, double lifeTimeInDay, double timeScale) {
      _avgDocCount = avgDocCount;
      _cpuModelA = cpuModelA;
      _cpuModelB = cpuModelB;
      _cpuModelAlpha = cpuModelAlpha;
      _lifeTimeInDay = lifeTimeInDay;
      _timeScale = timeScale;
    }

    public CPULoadFormulation(double avgDocCount, double cpuModelA, double cpuModelB, double cpuModelAlpha, double lifeTimeInDay, double timeScale, double docScannedModelP1, double docScannedModelP2, double docScannedModelP3) {
      _avgDocCount = avgDocCount;
      _cpuModelA = cpuModelA;
      _cpuModelB = cpuModelB;
      _cpuModelAlpha = cpuModelAlpha;
      _lifeTimeInDay = lifeTimeInDay;
      _timeScale = timeScale;

      _docScannedModelP1 = docScannedModelP1;
      _docScannedModelP2 = docScannedModelP2;
      _docScannedModelP3 = docScannedModelP3;
    }


    public double computeCPULoad(SegmentMetadata segmentMetadata) {

      //LOGGER.info("ComputingLoadFor: {}, {}, {}, {}, {}, {}", _avgDocCount, _a, _alpha, _b, _lifeTimeInDay, _timeScale);

      double lifeTimeInScaleSeconds = (_lifeTimeInDay * 24 * 3600)/_timeScale;

      double segmentMiddleTime = (segmentMetadata.getStartTime() + segmentMetadata.getEndTime()) / 2;
      double segmentAgeInScaleSeconds = ((System.currentTimeMillis()/1000) - segmentMiddleTime)/_timeScale;

      if(segmentAgeInScaleSeconds < 0)
      {
        segmentAgeInScaleSeconds = (1556668800 - segmentMiddleTime)/_timeScale;
      }

      if(segmentAgeInScaleSeconds < 0)
      {
        segmentAgeInScaleSeconds = 0;
      }

      if(segmentAgeInScaleSeconds > lifeTimeInScaleSeconds) {
        return 0;
      }

      double tmp = 1 - _cpuModelAlpha;

      LOGGER.info("ComputingLoadFor: {}, {}, {}, {}, {}", tmp,  _cpuModelA*(lifeTimeInScaleSeconds - segmentAgeInScaleSeconds), (_cpuModelB/tmp), Math.pow(lifeTimeInScaleSeconds,tmp), Math.pow(segmentAgeInScaleSeconds,tmp));
      //For monthly segments
      double segmentCost = _cpuModelA*(lifeTimeInScaleSeconds - segmentAgeInScaleSeconds) + (_cpuModelB/tmp)*(Math.pow(lifeTimeInScaleSeconds,tmp) - Math.pow(segmentAgeInScaleSeconds,tmp));

      //For daily segments

      segmentCost *= segmentMetadata.getTotalDocs()/_avgDocCount;
      return segmentCost;

      //return 0;
    }

    public double computeCPULoad(SegmentMetadata segmentMetadata, long maxTime) {

      //LOGGER.info("ComputingLoadFor: {}, {}, {}, {}, {}, {}, {}, {}", _avgDocCount, _cpuModelA,_cpuModelB,_cpuModelAlpha,_lifeTimeInDay,_timeScale,_docScannedModelP1,_docScannedModelP2,_docScannedModelP3);

      double lifeTimeInScaleSeconds = (_lifeTimeInDay * 24 * 3600)/_timeScale;

      double segmentMiddleTime = (segmentMetadata.getStartTime() + segmentMetadata.getEndTime()) / 2;
      double segmentAgeInScaleSeconds = (maxTime - segmentMiddleTime)/_timeScale;

      if(segmentAgeInScaleSeconds > lifeTimeInScaleSeconds) {
        return 0;
      }



      double tmp1 = 1 - _cpuModelAlpha;
      double tmp2 = 2 - _cpuModelAlpha;
      double tmp3 = 3 - _cpuModelAlpha;

      double tmp4 = (_docScannedModelP1/tmp3) * (Math.pow(lifeTimeInScaleSeconds,tmp3)-Math.pow(segmentAgeInScaleSeconds,tmp3));
      tmp4 += (_docScannedModelP2/tmp2) * (Math.pow(lifeTimeInScaleSeconds,tmp2)-Math.pow(segmentAgeInScaleSeconds,tmp2));
      tmp4 += (_docScannedModelP3/tmp1) * (Math.pow(lifeTimeInScaleSeconds,tmp1)-Math.pow(segmentAgeInScaleSeconds,tmp1));

      double tmp5 = (_docScannedModelP1/3)*(Math.pow(lifeTimeInScaleSeconds,3)-Math.pow(segmentAgeInScaleSeconds,3));
      tmp5 += (_docScannedModelP2/2)*(Math.pow(lifeTimeInScaleSeconds,2)-Math.pow(segmentAgeInScaleSeconds,2));
      tmp5 += (_docScannedModelP3)*(lifeTimeInScaleSeconds-segmentAgeInScaleSeconds);

      double perDocCost = (_cpuModelA * tmp4) + (_cpuModelB * tmp5);

      double segmentCost = segmentMetadata.getTotalDocs() * perDocCost;
      //LOGGER.info("DifferentLevelComputation: {}, {}, {}, {}, {}, {}, {}", tmp1,tmp2,tmp3,tmp4,tmp5,perDocCost,segmentCost);
      if(segmentCost < 0)
        segmentCost = 0;
      return segmentCost;

      //return 0;
    }

  }

}