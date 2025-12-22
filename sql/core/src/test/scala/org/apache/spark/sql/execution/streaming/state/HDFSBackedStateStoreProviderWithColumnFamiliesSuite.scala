/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.streaming.state

import java.util.UUID

import scala.util.Random

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.execution.streaming.runtime.StreamExecution
import org.apache.spark.sql.execution.streaming.state.StateStoreTestsHelper._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

class HDFSBackedStateStoreProviderWithColumnFamiliesSuite
  extends StateStoreSuiteBase[HDFSBackedStateStoreProvider] {

  override def newStoreProvider(): HDFSBackedStateStoreProvider = {
    newStoreProvider(opId = Random.nextInt(), partition = 0)
  }

  override def newStoreProvider(storeId: StateStoreId): HDFSBackedStateStoreProvider = {
    newStoreProvider(storeId.operatorId, storeId.partitionId, dir = storeId.checkpointRootLocation)
  }

  override def newStoreProvider(
      storeId: StateStoreId,
      conf: Configuration): HDFSBackedStateStoreProvider = {
    newStoreProvider(
      storeId.operatorId,
      storeId.partitionId,
      dir = storeId.checkpointRootLocation,
      hadoopConf = conf,
      useColumnFamilies = true)
  }

  override def newStoreProvider(
      storeId: StateStoreId,
      useColumnFamilies: Boolean): HDFSBackedStateStoreProvider = {
    newStoreProvider(storeId.operatorId, storeId.partitionId,
      dir = storeId.checkpointRootLocation, useColumnFamilies = useColumnFamilies)
  }

  override def newStoreProvider(
      minDeltasForSnapshot: Int,
      numOfVersToRetainInMemory: Int): HDFSBackedStateStoreProvider = {
    newStoreProvider(opId = Random.nextInt(), partition = 0,
      minDeltasForSnapshot = minDeltasForSnapshot,
      numOfVersToRetainInMemory = numOfVersToRetainInMemory)
  }

  override def newStoreProviderWithClonedConf(
      storeId: StateStoreId): HDFSBackedStateStoreProvider = {
    newStoreProvider(
      storeId.operatorId,
      storeId.partitionId,
      dir = storeId.checkpointRootLocation,
      sqlConfOpt = Some(cloneSQLConf()))
  }

  override def newStoreProviderNoInit(): HDFSBackedStateStoreProvider =
    new HDFSBackedStateStoreProvider

  override def getLatestData(
      storeProvider: HDFSBackedStateStoreProvider,
      useColumnFamilies: Boolean = false): Set[((String, Int), Int)] = {
    getData(storeProvider, -1, useColumnFamilies)
  }

  override def getData(
      provider: HDFSBackedStateStoreProvider,
      version: Int,
      useColumnFamilies: Boolean = false): Set[((String, Int), Int)] = {
    tryWithProviderResource(newStoreProvider(provider.stateStoreId, useColumnFamilies)) {
      reloadedProvider =>
      if (version < 0) {
        reloadedProvider.latestIterator().map(rowPairToDataPair).toSet
      } else {
        val store = reloadedProvider.getStore(version)
        try {
          store.iterator().map(rowPairToDataPair).toSet
        } finally {
          if (!store.hasCommitted) store.abort()
        }
      }
    }
  }

  def newStoreProvider(
      opId: Long,
      partition: Int,
      keyStateEncoderSpec: KeyStateEncoderSpec = NoPrefixKeyStateEncoderSpec(keySchema),
      keySchema: StructType = keySchema,
      dir: String = newDir(),
      sqlConfOpt: Option[SQLConf] = None,
      minDeltasForSnapshot: Int = SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.defaultValue.get,
      numOfVersToRetainInMemory: Int = SQLConf.MAX_BATCHES_TO_RETAIN_IN_MEMORY.defaultValue.get,
      hadoopConf: Configuration = new Configuration,
      useColumnFamilies: Boolean = true): HDFSBackedStateStoreProvider = {
    hadoopConf.set(StreamExecution.RUN_ID_KEY, UUID.randomUUID().toString)
    val sqlConf = sqlConfOpt.getOrElse(
      getDefaultSQLConf(minDeltasForSnapshot, numOfVersToRetainInMemory))
    val provider = new HDFSBackedStateStoreProvider()
    provider.init(
      StateStoreId(dir, opId, partition),
      keySchema,
      valueSchema,
      keyStateEncoderSpec,
      useColumnFamilies = useColumnFamilies,
      new StateStoreConf(sqlConf),
      hadoopConf)
    provider
  }

  override def newStoreProvider(
      keySchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      useColumnFamilies: Boolean): HDFSBackedStateStoreProvider = {
    newStoreProvider(opId = Random.nextInt(), partition = 0,
      keySchema = keySchema,
      keyStateEncoderSpec = keyStateEncoderSpec,
      useColumnFamilies = useColumnFamilies)
  }

  override def newStoreProvider(useColumnFamilies: Boolean): HDFSBackedStateStoreProvider = {
    newStoreProvider(opId = Random.nextInt(), partition = 0, useColumnFamilies = useColumnFamilies)
  }

  override def getDefaultSQLConf(
      minDeltasForSnapshot: Int,
      numOfVersToRetainInMemory: Int): SQLConf = {
    val sqlConf = new SQLConf()
    sqlConf.setConf(SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT, minDeltasForSnapshot)
    sqlConf.setConf(SQLConf.MAX_BATCHES_TO_RETAIN_IN_MEMORY, numOfVersToRetainInMemory)
    sqlConf.setConf(SQLConf.MIN_BATCHES_TO_RETAIN, 2)
    sqlConf.setConf(SQLConf.STATE_STORE_COMPRESSION_CODEC, SQLConf.get.stateStoreCompressionCodec)
    sqlConf.setConf(
      SQLConf.STREAMING_CHECKPOINT_FILE_CHECKSUM_ENABLED,
      SQLConf.get.checkpointFileChecksumEnabled)
    sqlConf.setConf(
      SQLConf.STATE_STORE_ROW_CHECKSUM_ENABLED, SQLConf.get.stateStoreRowChecksumEnabled)
    sqlConf
  }

  override def cloneSQLConf(): SQLConf = SQLConf.get.clone()
}
