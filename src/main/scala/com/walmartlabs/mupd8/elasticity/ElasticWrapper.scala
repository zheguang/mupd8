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

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.HashMap
import java.util.ArrayList
import com.walmartlabs.mupd8.Mupd8Utils
import com.walmartlabs.mupd8.application.binary.Updater;
import com.walmartlabs.mupd8.application.statistics.PrePerformer
import com.walmartlabs.mupd8.application.binary.PerformerUtilities

class ElasticWrapper(val updater: Updater, prePerformer: PrePerformer) extends Updater {

  val WRAPPER_SUFFIX = "_elastic_wrapper"
  var loadRedistributionInProgress = false
  var oracle: ElasticOracle = null
  var updateInProgress: AtomicBoolean = new AtomicBoolean(false)
  var buffer: HashMap[Float, ArrayList[Array[Byte]]] = new HashMap[Float, ArrayList[Array[Byte]]] //TODO concurrent hash map
  var keyHashes: HashMap[Float, Array[Byte]] = new HashMap[Float, Array[Byte]]

  override def getName(): String = {
    updater.getName() + WRAPPER_SUFFIX;
  }

  def getBufferedEvents() = { buffer }
  def getBufferedKeyHashes() = { keyHashes }

  def setLoadRedistributionInProgress(oracle: ElasticOracle): Unit = {
    //TODO : this.oracle = oracle
    loadRedistributionInProgress = true
    while (updateInProgress.get()) { Thread.sleep(5) }
    //TODO: ensure proper synchronization
  }

  def setLoadRedistributionCompletion(): Unit = {
	// iterate over the buffer and send the key events to 
    // the target directly
    
  }

  def actualUpdate(submitter: PerformerUtilities, stream: String, key: Array[Byte], event: Array[Byte], slate: Array[Byte]) = {
    prePerformer.prePerform(key, event)
    updateInProgress.set(true)
    updater.update(submitter, stream, key, event, slate)
    updateInProgress.set(false)
  }

  override def update(submitter: PerformerUtilities, stream: String, key: Array[Byte], event: Array[Byte], slate: Array[Byte]) = {

    if (loadRedistributionInProgress) {
      if (oracle.isMovingKey(key)) {
        var fl = Mupd8Utils.hash2Float(key)
        keyHashes += (fl -> key)
        var obj = buffer.getOrElse(fl, null)
        if (obj == null) {
          var list = new ArrayList[Array[Byte]]
          list.add(event)
          buffer.put(fl, list)
        } else {
          obj.asInstanceOf[ArrayList[Array[Byte]]].add(event)
        }
      } else {
        actualUpdate(submitter, stream, key, event, slate)
        // check if output needs to be bufferred 

      }
    } else {
      actualUpdate(submitter, stream, key, event, slate)
    }

  }



}
