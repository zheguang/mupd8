/**
 * Copyright 2011-2012 @WalmartLabs, a division of Wal-Mart Stores, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.walmartlabs.mupd8.elasticity

import com.walmartlabs.mupd8.application.statistics.NodeStatisticsReport
import com.walmartlabs.mupd8.AppRuntime
import com.walmartlabs.mupd8.Mupd8Utils
import com.walmartlabs.mupd8.MessageServerClient
import com.walmartlabs.mupd8.messaging.ActivityStatus._
import com.walmartlabs.mupd8.messaging.LoadReDistMessage
import com.walmartlabs.mupd8.messaging.ActivityStatus



class LoadDistributionPlanner(val appRuntime: AppRuntime) extends LoadPlanner {

  val maxHistory: Int = 10
  var nodeReports = Map[String, List[NodeStatisticsReport]]()
  val msgClient: MessageServerClient = appRuntime.getMessageServerClient()
  var avgLoad: Double = 0
  var reportCount: Int = 0
  val LOAD_THRESHOLD: Double = 3.0
  var leastLoadedHost: String = null
  var leastLoad: Double = 0;
  var loadRedistributionInProgress = false

  def receiveNodeStatisticsReport(report: NodeStatisticsReport): Unit = {
    var reports = nodeReports.getOrElse(report.getHostname(), List[NodeStatisticsReport]())
    if (reports == null) {
      reports = Nil
    }
    if (reports.size == maxHistory) {
      reports = reports.tail
    }
    reports = reports ::: List(report)

    if (leastLoadedHost == null) {
      leastLoadedHost = report.getHostname()
      leastLoad = report.getAvgLoad()
    } else {
      if (leastLoad > report.getAvgLoad()) {
        leastLoad = report.getAvgLoad()
        leastLoadedHost = report.getHostname()
      }
    }
    var transformation = analyzeNeedForRedistributionOfLoad(report)
    if (transformation != null && transformation.size > 0) {
      if (!loadRedistributionInProgress) {
        initiateLoadRedistribution(transformation)
        loadRedistributionInProgress = true
      } else {
        System.out.println(" not attempting to redistribute because a current transformation is in progress");
      }
    }
  }

  def analyzeNeedForRedistributionOfLoad(report: NodeStatisticsReport): Map[Float, Int] = {
    var transformation: Map[Float, Int] = null
    if (reportCount == 0) {
      avgLoad = report.getAvgLoad()
      reportCount += 1
      transformation
    } else {
      if (report.getAvgLoad() > 2 * avgLoad) {
        if (report.getHotKeys() != null && report.getHotKeys().length > 0) {
          transformation = redistributeHotKeys(report)
        } else {
          transformation = redistributeNodeLoad(report)
        }
      }
      avgLoad = (avgLoad * reportCount + report.getAvgLoad()) / (reportCount + 1)
      reportCount += 1
      transformation
    }
  }

  def redistributeHotKeys(report: NodeStatisticsReport): Map[Float, Int] = {
    var transformation = Map[Float, Int]()
    val ring = appRuntime.getHashRing()
    val hotKeys = report.getHotKeys()
    for (key <- hotKeys) {
      var revisedTarget = Mupd8Utils.getHostNameToIndex(appRuntime.getAppStaticInfo(), leastLoadedHost)
      transformation += (Mupd8Utils.hash2Float(key) -> revisedTarget)
    }
    transformation
  }

  def redistributeNodeLoad(report: NodeStatisticsReport): Map[Float, Int] = {
    // find out keys that are being routed to the loaded host
    var transformation = Map[Float, Int]()
    //transformation += (0.5f -> Mupd8Utils.getHostNameToIndex(appRuntime.getAppStaticInfo(), report.getHostname()))
    transformation += (0.5f -> 1)
    transformation
  }

  def initiateLoadRedistribution(transformation: Map[Float, Int]): Unit = {
    val redistMsg = new LoadReDistMessage(0,
      transformation, ActivityStatus.STARTED)
    msgClient.sendMessage(redistMsg)
  }

  def notifyLoadRedistributionActivityStatus(status: ActivityStatus): Unit = {
    status match {
      case FAILED => loadRedistributionInProgress = false
      case SUCCESS => loadRedistributionInProgress = false
    }

  }

}

