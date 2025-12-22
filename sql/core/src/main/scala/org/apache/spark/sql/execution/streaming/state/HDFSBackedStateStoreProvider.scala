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

import java.io._
import java.util
import java.util.{Locale, UUID}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, LongAdder}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.json4s.{DefaultFormats, Formats, JValue}
import org.json4s.JObject
import org.json4s.JsonAST.{JArray, JString}
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import org.apache.spark.{SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.streaming.checkpointing.{CheckpointFileManager, ChecksumCheckpointFileManager, ChecksumFile}
import org.apache.spark.sql.execution.streaming.checkpointing.CheckpointFileManager.CancellableFSDataOutputStream
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.util.{SizeEstimator, Utils}
import org.apache.spark.util.ArrayImplicits._


/**
 * An implementation of [[StateStoreProvider]] and [[StateStore]] in which all the data is backed
 * by files in an HDFS-compatible file system. All updates to the store has to be done in sets
 * transactionally, and each set of updates increments the store's version. These versions can
 * be used to re-execute the updates (by retries in RDD operations) on the correct version of
 * the store, and regenerate the store version.
 *
 * Usage:
 * To update the data in the state store, the following order of operations are needed.
 *
 * // get the right store
 * - val store = StateStore.get(
 *      StateStoreId(checkpointLocation, operatorId, partitionId), ..., version, ...)
 * - store.put(...)
 * - store.remove(...)
 * - store.commit()    // commits all the updates to made; the new version will be returned
 * - store.iterator()  // key-value data after last commit as an iterator
 *
 * Fault-tolerance model:
 * - Every set of updates is written to a delta file before committing.
 * - The state store is responsible for managing, collapsing and cleaning up of delta files.
 * - Multiple attempts to commit the same version of updates may overwrite each other.
 *   Consistency guarantees depend on whether multiple attempts have the same updates and
 *   the overwrite semantics of underlying file system.
 * - Background maintenance of files ensures that last versions of the store is always recoverable
 * to ensure re-executed RDD operations re-apply updates on the correct past version of the
 * store.
 */
private[sql] class HDFSBackedStateStoreProvider extends StateStoreProvider with Logging
  with SupportsFineGrainedReplay {

  private val providerName = "HDFSBackedStateStoreProvider"

  class HDFSBackedReadStateStore(
      val version: Long,
      map: Map[String, HDFSBackedStateStoreMap])
    extends ReadStateStore {

    override def id: StateStoreId = HDFSBackedStateStoreProvider.this.stateStoreId

    override def get(key: UnsafeRow, colFamilyName: String): UnsafeRow = {
      val stateStoreMap = map.getOrElse(colFamilyName,
        throw StateStoreErrors.unsupportedOperationOnMissingColumnFamily("get", colFamilyName))
      stateStoreMap.get(key)
    }

    override def iterator(colFamilyName: String): StateStoreIterator[UnsafeRowPair] = {
      val stateStoreMap = map.getOrElse(colFamilyName,
        throw StateStoreErrors.unsupportedOperationOnMissingColumnFamily("iterator", colFamilyName))
      val iter = stateStoreMap.iterator()
      new StateStoreIterator(iter)
    }

    override def abort(): Unit = {}

    override def release(): Unit = {}

    override def toString(): String = {
      s"HDFSReadStateStore[stateStoreId=$stateStoreId_, version=$version]"
    }

    override def prefixScan(
        prefixKey: UnsafeRow,
        colFamilyName: String): StateStoreIterator[UnsafeRowPair] = {
      val stateStoreMap = map.getOrElse(colFamilyName,
        throw StateStoreErrors.unsupportedOperationOnMissingColumnFamily("prefixScan",
          colFamilyName))
      val iter = stateStoreMap.prefixScan(prefixKey)
      new StateStoreIterator(iter)
    }

    override def valuesIterator(key: UnsafeRow, colFamilyName: String): Iterator[UnsafeRow] = {
      throw StateStoreErrors.unsupportedOperationException("multipleValuesPerKey", "HDFSStateStore")
    }

    override def allColumnFamilyNames: Set[String] = map.keySet
  }

  /** Implementation of [[StateStore]] API which is backed by an HDFS-compatible file system */
  class HDFSBackedStateStore(
      val version: Long,
      private val mapToUpdate: mutable.Map[String, HDFSBackedStateStoreMap],
      private val metadata: mutable.Map[String, ColFamilyMetadata],
      shouldForceSnapshot: Boolean = false)
    extends StateStore {

    /** Trait and classes representing the internal state of the store */
    trait STATE
    case object UPDATING extends STATE
    case object COMMITTED extends STATE
    case object ABORTED extends STATE
    case object RELEASED extends STATE

    Option(TaskContext.get()).foreach { ctxt =>
      ctxt.addTaskCompletionListener[Unit](ctx => {
        if (state == UPDATING) {
          abort()
        }
      })
    }

    private val newVersion = version + 1
    @volatile private var state: STATE = UPDATING
    private val finalDeltaFile: Path = deltaFile(newVersion)
    private lazy val deltaFileStream = fm.createAtomic(finalDeltaFile, overwriteIfPossible = true)
    private lazy val compressedStream = compressStream(deltaFileStream)

    override def id: StateStoreId = HDFSBackedStateStoreProvider.this.stateStoreId

    override def createColFamilyIfAbsent(
        colFamilyName: String,
        keySchema: StructType,
        valueSchema: StructType,
        keyStateEncoderSpec: KeyStateEncoderSpec,
        useMultipleValuesPerKey: Boolean = false,
        isInternal: Boolean = false): Unit = {
      if (!mapToUpdate.contains(colFamilyName)) {
        val verifier = KeyValueIntegrityVerifier.create(
          stateStoreId_.toString,
          storeConf.rowChecksumEnabled,
          storeConf.rowChecksumReadVerificationRatio)
        val numColsPrefixKey = getNumColsPrefixKey(keyStateEncoderSpec)
        val newMap = HDFSBackedStateStoreMap.create(keySchema, numColsPrefixKey, verifier)
        mapToUpdate.put(colFamilyName, newMap)
        metadata.put(colFamilyName, ColFamilyMetadata(colFamilyName, keySchema, valueSchema,
          keyStateEncoderSpec, numColsPrefixKey))
      }
    }

    override def allColumnFamilyNames: Set[String] = mapToUpdate.keySet.toSet

    // Multiple col families are not supported with HDFSBackedStateStoreProvider. Throw an exception
    // if the user tries to use a non-default col family.
    private def assertUseOfDefaultColFamily(colFamilyName: String): Unit = {
      if (!useColumnFamilies && colFamilyName != StateStore.DEFAULT_COL_FAMILY_NAME) {
        throw StateStoreErrors.multipleColumnFamiliesNotSupported(providerName)
      }
    }

    override def get(key: UnsafeRow, colFamilyName: String): UnsafeRow = {
      assertUseOfDefaultColFamily(colFamilyName)
      val stateStoreMap = mapToUpdate.getOrElse(colFamilyName,
        throw StateStoreErrors.unsupportedOperationOnMissingColumnFamily("get", colFamilyName))
      stateStoreMap.get(key)
    }

    override def put(key: UnsafeRow, value: UnsafeRow, colFamilyName: String): Unit = {
      assertUseOfDefaultColFamily(colFamilyName)
      val stateStoreMap = mapToUpdate.getOrElse(colFamilyName,
        throw StateStoreErrors.unsupportedOperationOnMissingColumnFamily("put", colFamilyName))
      require(value != null, "Cannot put a null value")
      verify(state == UPDATING, "Cannot put after already committed or aborted")
      val keyCopy = key.copy()
      val valueCopy = value.copy()

      val valueWrapper = if (storeConf.rowChecksumEnabled) {
        // Add the key-value checksum to the value row
        StateStoreRowWithChecksum(valueCopy, KeyValueChecksum.create(keyCopy, Some(valueCopy)))
      } else {
        StateStoreRow(valueCopy)
      }

      stateStoreMap.put(keyCopy, valueWrapper)

      val stream = getOrCreateDeltaStream(colFamilyName)
      writeUpdateToDeltaFile(stream, keyCopy, valueWrapper)
    }

    private val deltaStreams = mutable.Map[String, DataOutputStream]()
    private val rawDeltaStreams = mutable.Map[String, CancellableFSDataOutputStream]()

    private def getOrCreateDeltaStream(colFamilyName: String): DataOutputStream = {
      if (colFamilyName == StateStore.DEFAULT_COL_FAMILY_NAME) {
        compressedStream
      } else {
        deltaStreams.getOrElseUpdate(colFamilyName, {
          val file = deltaFile(newVersion, colFamilyName)
          val rawStream = fm.createAtomic(file, overwriteIfPossible = true)
          rawDeltaStreams.put(colFamilyName, rawStream)
          compressStream(rawStream)
        })
      }
    }

    override def remove(key: UnsafeRow, colFamilyName: String): Unit = {
      assertUseOfDefaultColFamily(colFamilyName)
      val stateStoreMap = mapToUpdate.getOrElse(colFamilyName,
        throw StateStoreErrors.unsupportedOperationOnMissingColumnFamily("remove", colFamilyName))
      verify(state == UPDATING, "Cannot remove after already committed or aborted")
      val prevValue = stateStoreMap.remove(key)
      if (prevValue != null) {
        val keyWrapper = if (storeConf.rowChecksumEnabled) {
          // Add checksum for only the removed key
          StateStoreRowWithChecksum(key, KeyValueChecksum.create(key, None))
        } else {
          StateStoreRow(key)
        }
        val stream = getOrCreateDeltaStream(colFamilyName)
        writeRemoveToDeltaFile(stream, keyWrapper)
      }
    }

    /** Commit all the updates that have been made to the store, and return the new version. */
    override def commit(): Long = {
      try {
        verify(state == UPDATING, "Cannot commit after already committed or aborted")

        // Finalize all auxiliary delta streams
        deltaStreams.foreach { case (_, stream) =>
          finalizeDeltaFile(stream)
        }

        // Write metadata
        writeMetadata(newVersion, metadata.toMap)

        commitUpdates(newVersion, mapToUpdate.toMap, compressedStream, shouldForceSnapshot)
        state = COMMITTED
        logInfo(log"Committed version ${MDC(LogKeys.COMMITTED_VERSION, newVersion)} " +
          log"for ${MDC(LogKeys.STATE_STORE_PROVIDER, this)} to file " +
          log"${MDC(LogKeys.FILE_NAME, finalDeltaFile)}")

        // Report the commit to StateStoreCoordinator for tracking
        if (storeConf.commitValidationEnabled) {
          StateStore.reportCommitToCoordinator(newVersion, stateStoreId, hadoopConf)
        }

        newVersion
      } catch {
        case e: Throwable =>
          throw QueryExecutionErrors.failedToCommitStateFileError(this.toString(), e)
      }
    }

    /** Abort all the updates made on this store. This store will not be usable any more. */
    override def abort(): Unit = {
      // This if statement is to ensure that files are deleted only once: if either commit or abort
      // is called before, it will be no-op.
      if (state == UPDATING) {
        state = ABORTED
        cancelDeltaFile(compressedStream, deltaFileStream)
        // Abort auxiliary streams
        deltaStreams.foreach { case (cfName, stream) =>
          val rawStream = rawDeltaStreams.get(cfName).orNull
          cancelDeltaFile(stream, rawStream)
        }
      } else {
        state = ABORTED
      }
      logInfo(log"Aborted version ${MDC(LogKeys.STATE_STORE_VERSION, newVersion)} " +
        log"for ${MDC(LogKeys.STATE_STORE_PROVIDER, this)}")
    }

    /**
     * Get an iterator of all the store data.
     * This can be called only after committing all the updates made in the current thread.
     */
    override def iterator(colFamilyName: String): StateStoreIterator[UnsafeRowPair] = {
      assertUseOfDefaultColFamily(colFamilyName)
      val stateStoreMap = mapToUpdate.getOrElse(colFamilyName,
        throw StateStoreErrors.unsupportedOperationOnMissingColumnFamily("iterator", colFamilyName))
      val iter = stateStoreMap.iterator()
      new StateStoreIterator(iter)
    }

    override def prefixScan(
        prefixKey: UnsafeRow,
        colFamilyName: String): StateStoreIterator[UnsafeRowPair] = {
      assertUseOfDefaultColFamily(colFamilyName)
      val stateStoreMap = mapToUpdate.getOrElse(colFamilyName,
        throw StateStoreErrors.unsupportedOperationOnMissingColumnFamily("prefixScan",
          colFamilyName))
      val iter = stateStoreMap.prefixScan(prefixKey)
      new StateStoreIterator(iter)
    }

    override def metrics: StateStoreMetrics = {
      // NOTE: we provide estimation of cache size as "memoryUsedBytes", and size of state for
      // current version as "stateOnCurrentVersionSizeBytes"
      val metricsFromProvider: Map[String, Long] = getMetricsForProvider()

      // Calculate total size of all maps
      val totalSize = mapToUpdate.values.map(m => SizeEstimator.estimate(m)).sum
      val totalKeys = mapToUpdate.values.map(_.size().toLong).sum

      val customMetrics = metricsFromProvider.flatMap { case (name, value) =>
        // just allow searching from list cause the list is small enough
        supportedCustomMetrics.find(_.name == name).map(_ -> value)
      } + (metricStateOnCurrentVersionSizeBytes -> totalSize) +
        (metricForceSnapshot -> (if (shouldForceSnapshot) 1L else 0L))

      val instanceMetrics = Map(
        instanceMetricSnapshotLastUpload.withNewId(
          stateStoreId.partitionId, stateStoreId.storeName) -> lastUploadedSnapshotVersion.get()
      )

      StateStoreMetrics(
        totalKeys,
        metricsFromProvider("memoryUsedBytes"),
        customMetrics,
        instanceMetrics
      )
    }

    override def getStateStoreCheckpointInfo(): StateStoreCheckpointInfo = {
      StateStoreCheckpointInfo(id.partitionId, newVersion, None, None)
    }

    /**
     * Whether all updates have been committed
     */
    override def hasCommitted: Boolean = {
      state == COMMITTED
    }

    override def toString(): String = {
      s"HDFSStateStore[stateStoreId=$stateStoreId_, version=$version]"
    }

    override def removeColFamilyIfExists(colFamilyName: String): Boolean = {
      if (mapToUpdate.contains(colFamilyName)) {
        mapToUpdate.remove(colFamilyName)
        metadata.remove(colFamilyName)
        true
      } else {
        false
      }
    }

    override def valuesIterator(key: UnsafeRow, colFamilyName: String): Iterator[UnsafeRow] = {
      throw StateStoreErrors.unsupportedOperationException("multipleValuesPerKey", providerName)
    }

    override def putList(key: UnsafeRow, values: Array[UnsafeRow], colFamilyName: String): Unit = {
      throw StateStoreErrors.unsupportedOperationException("putList", providerName)
    }

    override def merge(key: UnsafeRow,
        value: UnsafeRow,
        colFamilyName: String): Unit = {
      throw StateStoreErrors.unsupportedOperationException("merge", providerName)
    }

    override def mergeList(
        key: UnsafeRow, values: Array[UnsafeRow], colFamilyName: String): Unit = {
      throw StateStoreErrors.unsupportedOperationException("mergeList", providerName)
    }
  }

  def getMetricsForProvider(): Map[String, Long] = synchronized {
    Map("memoryUsedBytes" -> SizeEstimator.estimate(loadedMaps),
      metricLoadedMapCacheHit.name -> loadedMapCacheHitCount.sum(),
      metricLoadedMapCacheMiss.name -> loadedMapCacheMissCount.sum(),
      metricNumSnapshotsAutoRepaired.name -> (if (performedSnapshotAutoRepair.get()) 1 else 0)
    )
  }

  /** Get the state store for making updates to create a new `version` of the store. */
  override def getStore(
      version: Long,
      uniqueId: Option[String] = None,
      forceSnapshotOnCommit: Boolean = false,
      loadEmpty: Boolean = false): StateStore = {
    if (uniqueId.isDefined) {
      throw StateStoreErrors.stateStoreCheckpointIdsNotSupported(
        "HDFSBackedStateStoreProvider does not support checkpointFormatVersion > 1 " +
        "but a state store checkpointID is passed in")
    }
    if (loadEmpty) {
      throw StateStoreErrors.unsupportedOperationException("getStore",
        "Internal Error: HDFSBackedStateStoreProvider doesn't support loadEmpty")
    }
    val (newMaps, metadata) = getLoadedMapForStore(version)
    logInfo(log"Retrieved version ${MDC(LogKeys.STATE_STORE_VERSION, version)} " +
      log"of ${MDC(LogKeys.STATE_STORE_PROVIDER, HDFSBackedStateStoreProvider.this)} " +
      log"for update, forceSnapshotOnCommit=" +
      log"${MDC(LogKeys.STREAM_SHOULD_FORCE_SNAPSHOT, forceSnapshotOnCommit)}")
    new HDFSBackedStateStore(version, newMaps, metadata, forceSnapshotOnCommit)
  }

  /** Get the state store for reading to specific `version` of the store. */
  override def getReadStore(version: Long, stateStoreCkptId: Option[String]): ReadStateStore = {
    val (newMaps, _) = getLoadedMapForStore(version)
    logInfo(log"Retrieved version ${MDC(LogKeys.STATE_STORE_VERSION, version)} of " +
      log"${MDC(LogKeys.STATE_STORE_PROVIDER, HDFSBackedStateStoreProvider.this)} for readonly")
    // Convert mutable map to immutable for ReadStateStore
    new HDFSBackedReadStateStore(version, newMaps.toMap)
  }

  private def getLoadedMapForStore(version: Long): (mutable.Map[String, HDFSBackedStateStoreMap],
    mutable.Map[String, ColFamilyMetadata]) = synchronized {
    try {
      if (version < 0) {
        throw QueryExecutionErrors.unexpectedStateStoreVersion(version)
      }

      performedSnapshotAutoRepair.set(false)

      val (loadedMaps, loadedMetadata) = if (version > 0) {
        val loaded = loadMap(version)
        (loaded.maps, loaded.metadata)
      } else {
        (Map.empty[String, HDFSBackedStateStoreMap], Map.empty[String, ColFamilyMetadata])
      }

      val newMaps = mutable.Map[String, HDFSBackedStateStoreMap]()

      val newMetadata = mutable.Map[String, ColFamilyMetadata]()
      newMetadata ++= loadedMetadata

      if (newMetadata.isEmpty && version == 0) {
         val defaultMetadata = ColFamilyMetadata(StateStore.DEFAULT_COL_FAMILY_NAME,
           keySchema, valueSchema, keyStateEncoderSpec, numColsPrefixKey)
         newMetadata.put(StateStore.DEFAULT_COL_FAMILY_NAME, defaultMetadata)

         val verifier = KeyValueIntegrityVerifier.create(
          stateStoreId_.toString,
          storeConf.rowChecksumEnabled,
          storeConf.rowChecksumReadVerificationRatio)
         val newMap = HDFSBackedStateStoreMap.create(keySchema, numColsPrefixKey, verifier)
         newMaps.put(StateStore.DEFAULT_COL_FAMILY_NAME, newMap)
      } else {
         loadedMaps.foreach { case (cfName, map) =>
            val verifier = KeyValueIntegrityVerifier.create(
              stateStoreId_.toString,
              storeConf.rowChecksumEnabled,
              storeConf.rowChecksumReadVerificationRatio)

            val metadata = loadedMetadata.getOrElse(cfName,
               throw new IllegalStateException(s"Missing metadata for column family $cfName"))

            val newMap = HDFSBackedStateStoreMap.create(metadata.keySchema,
               metadata.numColsPrefixKey, verifier)
            newMap.putAll(map)
            newMaps.put(cfName, newMap)
         }
      }

      (newMaps, newMetadata)
    }
    catch {
      case e: OutOfMemoryError =>
        throw QueryExecutionErrors.notEnoughMemoryToLoadStore(
          stateStoreId.toString,
          "HDFS_STORE_PROVIDER",
          e)
      case e: Throwable => throw StateStoreErrors.cannotLoadStore(e)
    }
  }

  // Run bunch of validations specific to HDFSBackedStateStoreProvider
  private def runValidation(
      useColumnFamilies: Boolean,
      useMultipleValuesPerKey: Boolean,
      keyStateEncoderSpec: KeyStateEncoderSpec): Unit = {

    if (useMultipleValuesPerKey) {
      throw StateStoreErrors.unsupportedOperationException("multipleValuesPerKey", providerName)
    }

    if (keyStateEncoderSpec.isInstanceOf[RangeKeyScanStateEncoderSpec]) {
      throw StateStoreErrors.unsupportedOperationException("Range scan", providerName)
    }
  }

  private def getNumColsPrefixKey(keyStateEncoderSpec: KeyStateEncoderSpec): Int = {
    keyStateEncoderSpec match {
      case NoPrefixKeyStateEncoderSpec(_) => 0
      case PrefixKeyScanStateEncoderSpec(_, numColsPrefixKey) => numColsPrefixKey
      case _ => throw StateStoreErrors.unsupportedOperationException("Invalid key state encoder",
        providerName)
    }
  }

  override def init(
      stateStoreId: StateStoreId,
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      useColumnFamilies: Boolean,
      storeConf: StateStoreConf,
      hadoopConf: Configuration,
      useMultipleValuesPerKey: Boolean = false,
      stateSchemaProvider: Option[StateSchemaProvider] = None): Unit = {
    if (storeConf.enableStateStoreCheckpointIds) {
      throw StateStoreErrors.stateStoreCheckpointIdsNotSupported(
        "HDFSBackedStateStoreProvider does not support checkpointFormatVersion > 1")
    }

    this.stateStoreId_ = stateStoreId
    this.keySchema = keySchema
    this.valueSchema = valueSchema
    this.keyStateEncoderSpec = keyStateEncoderSpec
    this.useColumnFamilies = useColumnFamilies
    this.storeConf = storeConf
    this.hadoopConf = hadoopConf
    this.numberOfVersionsToRetainInMemory = storeConf.maxVersionsToRetainInMemory

    val queryRunId = UUID.fromString(StateStoreProvider.getRunId(hadoopConf))
    this.stateStoreProviderId = StateStoreProviderId(stateStoreId, queryRunId)

    // run a bunch of validation checks for this state store provider
    runValidation(useColumnFamilies, useMultipleValuesPerKey, keyStateEncoderSpec)

    this.numColsPrefixKey = getNumColsPrefixKey(keyStateEncoderSpec)

    fm.mkdirs(baseDir)
  }

  override def stateStoreId: StateStoreId = stateStoreId_

  override protected def logName: String = s"${super.logName} ${stateStoreProviderId}"

  /** Do maintenance backing data files, including creating snapshots and cleaning up old files */
  override def doMaintenance(): Unit = {
    doSnapshot("maintenance")
    cleanup()
  }

  override def close(): Unit = {
    // Clearing the map resets the TreeMap.root to null, and therefore entries inside the
    // `loadedMaps` will be de-referenced and GCed automatically when their reference
    // counts become 0.
    synchronized { loadedMaps.clear() }
    fm.close()
  }

  override def supportedCustomMetrics: Seq[StateStoreCustomMetric] = {
    metricStateOnCurrentVersionSizeBytes :: metricLoadedMapCacheHit :: metricLoadedMapCacheMiss ::
    metricNumSnapshotsAutoRepaired :: metricForceSnapshot :: Nil
  }

  override def supportedInstanceMetrics: Seq[StateStoreInstanceMetric] =
    Seq(instanceMetricSnapshotLastUpload)

  override def toString(): String = {
    s"HDFSStateStoreProvider[stateStoreProviderId=$stateStoreProviderId]"
  }

  /* Internal fields and methods */

  @volatile private var stateStoreId_ : StateStoreId = _
  @volatile private var keySchema: StructType = _
  @volatile private var valueSchema: StructType = _
  @volatile private var keyStateEncoderSpec: KeyStateEncoderSpec = _
  @volatile private var useColumnFamilies: Boolean = _
  @volatile private var storeConf: StateStoreConf = _
  @volatile private var hadoopConf: Configuration = _
  @volatile private var numberOfVersionsToRetainInMemory: Int = _
  @volatile private var numColsPrefixKey: Int = 0
  @volatile private var stateStoreProviderId: StateStoreProviderId = _

  // TODO: The validation should be moved to a higher level so that it works for all state store
  // implementations
  @volatile private var isValidated = false

  import HDFSBackedStateStoreProvider.LoadedMapEntry

  private lazy val loadedMaps = new util.TreeMap[Long, LoadedMapEntry](Ordering[Long].reverse)
  private lazy val baseDir = stateStoreId.storeCheckpointLocation()
  // Visible to state pkg for testing.
  private[state] lazy val fm = {
    val mgr = CheckpointFileManager.create(baseDir, hadoopConf)
    if (storeConf.checkpointFileChecksumEnabled) {
      new ChecksumCheckpointFileManager(
        mgr,
        // Allowing this for perf, since we do orphan checksum file cleanup in maintenance anyway
        allowConcurrentDelete = true,
        // We need 2 threads per fm caller to avoid blocking
        // (one for main file and another for checksum file).
        // Since this fm is used by both query task and maintenance thread,
        // then we need 2 * 2 = 4 threads.
        numThreads = 4,
        skipCreationIfFileMissingChecksum =
          storeConf.checkpointFileChecksumSkipCreationIfFileMissingChecksum)
    } else {
      mgr
    }
  }
  private val onlySnapshotFiles = new PathFilter {
    override def accept(path: Path): Boolean = path.toString.endsWith(".snapshot")
  }
  private lazy val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf)

  private val loadedMapCacheHitCount: LongAdder = new LongAdder
  private val loadedMapCacheMissCount: LongAdder = new LongAdder

  // This is updated when the maintenance task writes the snapshot file and read by the task
  // thread. -1 represents no version has ever been uploaded.
  private val lastUploadedSnapshotVersion: AtomicLong = new AtomicLong(-1L)
  // Was snapshot auto repair performed when loading the current version
  private val performedSnapshotAutoRepair: AtomicBoolean = new AtomicBoolean(false)

  private lazy val metricStateOnCurrentVersionSizeBytes: StateStoreCustomSizeMetric =
    StateStoreCustomSizeMetric("stateOnCurrentVersionSizeBytes",
      "estimated size of state only on current version")

  private lazy val metricLoadedMapCacheHit: StateStoreCustomMetric =
    StateStoreCustomSumMetric("loadedMapCacheHitCount",
      "count of cache hit on states cache in provider")

  private lazy val metricLoadedMapCacheMiss: StateStoreCustomMetric =
    StateStoreCustomSumMetric("loadedMapCacheMissCount",
      "count of cache miss on states cache in provider")

  private lazy val metricNumSnapshotsAutoRepaired: StateStoreCustomMetric =
    StateStoreCustomSumMetric("numSnapshotsAutoRepaired",
    "number of snapshots that were automatically repaired during store load")

  private lazy val metricForceSnapshot: StateStoreCustomMetric =
    StateStoreCustomSumMetric("forceSnapshotCount",
    "number of stores that had forced snapshot")

  private lazy val instanceMetricSnapshotLastUpload: StateStoreInstanceMetric =
    StateStoreSnapshotLastUploadInstanceMetric()

  private case class StoreFile(version: Long, path: Path, isSnapshot: Boolean)

  private def commitUpdates(
      newVersion: Long,
      maps: Map[String, HDFSBackedStateStoreMap],
      output: DataOutputStream,
      shouldForceSnapshot: Boolean = false): Unit = {
    synchronized {
      finalizeDeltaFile(output)
      if (shouldForceSnapshot) {
        // Write snapshot for ALL maps
        maps.foreach { case (cfName, map) =>
          writeSnapshotFile(newVersion, map, "commit", cfName)
        }
      }
      putStateIntoStateCacheMap(newVersion, maps)
    }
  }

  /**
   * Get iterator of all the data of the latest version of the store.
   * Note that this will look up the files to determined the latest known version.
   */
  private[state] def latestIterator(): Iterator[UnsafeRowPair] = synchronized {
    val storeFiles = fetchFiles()._1
    val versionsInFiles = storeFiles.map(_.version).toSet
    val versionsLoaded = loadedMaps.keySet.asScala
    val allKnownVersions = versionsInFiles ++ versionsLoaded
    if (allKnownVersions.nonEmpty) {
      val loaded = loadMap(allKnownVersions.max)
      // For backward compatibility of tests, we return iterator of default CF
      loaded.maps.get(StateStore.DEFAULT_COL_FAMILY_NAME).
        map(_.iterator()).getOrElse(Iterator.empty)
    } else Iterator.empty
  }

  /** This method is intended to be only used for unit test(s). DO NOT TOUCH ELEMENTS IN MAP! */
  private[state] def getLoadedMaps(): util.SortedMap[Long, HDFSBackedStateStoreMap] = synchronized {
    // shallow copy as a minimal guard
    val result = new util.TreeMap[Long, HDFSBackedStateStoreMap]()
    loadedMaps.asScala.foreach { case (version, entry) =>
      if (entry.maps.contains(StateStore.DEFAULT_COL_FAMILY_NAME)) {
        result.put(version, entry.maps(StateStore.DEFAULT_COL_FAMILY_NAME))
      }
    }
    result
  }

  private def putStateIntoStateCacheMap(
      newVersion: Long,
      maps: Map[String, HDFSBackedStateStoreMap]): Unit = synchronized {

    val metadataMap = readMetadata(newVersion).getOrElse(Map.empty)

    val loadedEntries = loadedMaps.size()
    val earliestLoadedVersion: Option[Long] = if (loadedEntries > 0) {
      Some(loadedMaps.lastKey())
    } else {
      None
    }

    if (earliestLoadedVersion.isDefined) {
      logInfo(log"Trying to add version=${MDC(LogKeys.STATE_STORE_VERSION, newVersion)} to state " +
        log"cache map with current_size=${MDC(LogKeys.NUM_LOADED_ENTRIES, loadedEntries)} and " +
        log"earliest_loaded_version=" +
        log"${MDC(LogKeys.EARLIEST_LOADED_VERSION, earliestLoadedVersion.get)} " +
        log"and max_versions_to_retain_in_memory=" +
        log"${MDC(LogKeys.NUM_VERSIONS_RETAIN, numberOfVersionsToRetainInMemory)}")
    } else {
      logInfo(log"Trying to add version=${MDC(LogKeys.STATE_STORE_VERSION, newVersion)} to state " +
        log"cache map with current_size=${MDC(LogKeys.NUM_LOADED_ENTRIES, loadedEntries)} and " +
        log"max_versions_to_retain_in_memory=" +
        log"${MDC(LogKeys.NUM_VERSIONS_RETAIN, numberOfVersionsToRetainInMemory)}")
    }

    if (numberOfVersionsToRetainInMemory <= 0) {
      if (loadedMaps.size() > 0) loadedMaps.clear()
      return
    }

    while (loadedMaps.size() > numberOfVersionsToRetainInMemory) {
      loadedMaps.remove(loadedMaps.lastKey())
    }

    val size = loadedMaps.size()
    if (size == numberOfVersionsToRetainInMemory) {
      val versionIdForLastKey = loadedMaps.lastKey()
      if (versionIdForLastKey > newVersion) {
        // this is the only case which we can avoid putting, because new version will be placed to
        // the last key and it should be evicted right away
        return
      } else if (versionIdForLastKey < newVersion) {
        // this case needs removal of the last key before putting new one
        loadedMaps.remove(versionIdForLastKey)
      }
    }

    loadedMaps.put(newVersion, LoadedMapEntry(maps, metadataMap))
  }

  /** Load the required version of the map data from the backing files */
  private def loadMap(version: Long): LoadedMapEntry = {

    // Shortcut if the map for this version is already there to avoid a redundant put.
    val loadedCurrentVersionMap = synchronized { Option(loadedMaps.get(version)) }
    if (loadedCurrentVersionMap.isDefined) {
      loadedMapCacheHitCount.increment()
      return loadedCurrentVersionMap.get
    }

    logWarning(log"The state for version ${MDC(LogKeys.FILE_VERSION, version)} doesn't exist in " +
      log"loadedMaps. Reading snapshot file and delta files if needed..." +
      log"Note that this is normal for the first batch of starting query.")

    loadedMapCacheMissCount.increment()

    val (result, elapsedMs) = Utils.timeTakenMs {
      val (loadedVersion, snapshotMaps, loadedMetadata) = loadSnapshot(version)

      val finalMetadata = readMetadata(version).getOrElse {
        // Fallback for backward compatibility or missing metadata
        if (version == loadedVersion) loadedMetadata
        else if (version == 0) {
           Map(StateStore.DEFAULT_COL_FAMILY_NAME ->
             ColFamilyMetadata(StateStore.DEFAULT_COL_FAMILY_NAME, keySchema, valueSchema,
               keyStateEncoderSpec, numColsPrefixKey))
        } else {
             // Default to default CF if metadata is missing (legacy support)
             Map(StateStore.DEFAULT_COL_FAMILY_NAME ->
               ColFamilyMetadata(StateStore.DEFAULT_COL_FAMILY_NAME, keySchema, valueSchema,
                 keyStateEncoderSpec, numColsPrefixKey))
        }
      }

      val finalMaps = if (loadedVersion == version) {
        snapshotMaps
      } else {
        val mutableMaps = mutable.Map[String, HDFSBackedStateStoreMap]()

        finalMetadata.foreach { case (cfName, metadata) =>
             val verifier = KeyValueIntegrityVerifier.create(
              stateStoreId_.toString,
              storeConf.rowChecksumEnabled,
              storeConf.rowChecksumReadVerificationRatio)
             val newMap = HDFSBackedStateStoreMap.create(metadata.keySchema,
                 metadata.numColsPrefixKey, verifier)

             if (snapshotMaps.contains(cfName)) {
                newMap.putAll(snapshotMaps(cfName))
             }
             mutableMaps.put(cfName, newMap)
        }

        for (deltaVersion <- loadedVersion + 1 to version) {
          updateFromDeltaFile(deltaVersion, mutableMaps, finalMetadata)
        }
        mutableMaps.toMap
      }

      val resultEntry = LoadedMapEntry(finalMaps, finalMetadata)
      // Synchronize and update the state cache map
      synchronized {
        loadedMaps.put(version, resultEntry)
        putStateIntoStateCacheMap(version, finalMaps)
      }

      // Report the snapshot found to the coordinator
      reportSnapshotUploadToCoordinator(loadedVersion)

      resultEntry
    }

    logDebug(s"Loading state for $version takes $elapsedMs ms.")

    result
  }

  /** Loads the latest snapshot for the version we want to load and
   * returns the snapshot version and map representing the snapshot */
  private def loadSnapshot(versionToLoad: Long): (Long, Map[String, HDFSBackedStateStoreMap],
      Map[String, ColFamilyMetadata]) = {
    var loadedMapsResult: Option[LoadedMapEntry] = None
    val storeIdStr = s"StateStoreId(opId=${stateStoreId_.operatorId}," +
      s"partId=${stateStoreId_.partitionId},name=${stateStoreId_.storeName})"

    val snapshotLoader = new AutoSnapshotLoader(
      storeConf.autoSnapshotRepairEnabled,
      storeConf.autoSnapshotRepairNumFailuresBeforeActivating,
      storeConf.autoSnapshotRepairMaxChangeFileReplay,
      storeIdStr) {
      override protected def beforeLoad(): Unit = {}

      override protected def loadSnapshotFromCheckpoint(snapshotVersion: Long): Unit = {
        loadedMapsResult = if (snapshotVersion <= 0) {
          // Use an empty map for versions 0 or less.
          Some(LoadedMapEntry(Map.empty, Map.empty))
        } else {
          // first try to get the map from the cache
          synchronized { Option(loadedMaps.get(snapshotVersion)) }
            .orElse {
               val metadataMap = readMetadata(snapshotVersion).getOrElse {
                   // Fallback for backward compatibility or missing metadata
                   Map(StateStore.DEFAULT_COL_FAMILY_NAME ->
                     ColFamilyMetadata(StateStore.DEFAULT_COL_FAMILY_NAME, keySchema, valueSchema,
                       keyStateEncoderSpec, numColsPrefixKey))
               }

               val maps = mutable.Map[String, HDFSBackedStateStoreMap]()
               var anySnapshotFound = false
               metadataMap.foreach { case (cfName, metadata) =>
                   readSnapshotFile(snapshotVersion, metadata, cfName).foreach { map =>
                      maps.put(cfName, map)
                      anySnapshotFound = true
                   }
               }

               if (maps.isEmpty && metadataMap.nonEmpty && snapshotVersion > 0) {
                 None
               } else {
                 Some(LoadedMapEntry(maps.toMap, metadataMap))
               }
            }
        }
      }

      override protected def onLoadSnapshotFromCheckpointFailure(): Unit = {}

      override protected def getEligibleSnapshots(versionToLoad: Long): Seq[Long] = {
        val snapshotVersions = SnapshotLoaderHelper.getEligibleSnapshotsForVersion(
          versionToLoad, fm, baseDir, onlySnapshotFiles, fileSuffix = ".snapshot")

        // Get locally cached versions, so we can use the locally cached version if available.
        val cachedVersions = synchronized {
          loadedMaps.keySet.asScala.toSeq
        }.filter(_ <= versionToLoad)

        // Combine the two sets of versions, so we can check both during load
        (snapshotVersions ++ cachedVersions).distinct
      }
    }

    val (loadedVersion, autoRepairCompleted) = snapshotLoader.loadSnapshot(versionToLoad)
    performedSnapshotAutoRepair.set(autoRepairCompleted)
    val entry = loadedMapsResult.get
    (loadedVersion, entry.maps, entry.metadata)
  }

  private def writeUpdateToDeltaFile(
      output: DataOutputStream,
      key: UnsafeRow,
      value: UnsafeRowWrapper): Unit = {
    val keyBytes = key.getBytes()
    val valueBytes = value match {
      case v: StateStoreRowWithChecksum =>
        // If it has checksum, encode the value bytes with the checksum.
        KeyValueChecksumEncoder.encodeSingleValueRowWithChecksum(
          v.unsafeRow().getBytes(), v.checksum)
      case _ => value.unsafeRow().getBytes()
    }

    output.writeInt(keyBytes.size)
    output.write(keyBytes)
    output.writeInt(valueBytes.size)
    output.write(valueBytes)
  }

  private def writeRemoveToDeltaFile(output: DataOutputStream, key: UnsafeRowWrapper): Unit = {
    val keyBytes = key match {
      case k: StateStoreRowWithChecksum =>
        // If it has checksum, encode the key bytes with the checksum.
        KeyValueChecksumEncoder.encodeKeyRowWithChecksum(
          k.unsafeRow().getBytes(), k.checksum)
      case _ => key.unsafeRow().getBytes()
    }
    output.writeInt(keyBytes.size)
    output.write(keyBytes)
    output.writeInt(-1)
  }

  private def finalizeDeltaFile(output: DataOutputStream): Unit = {
    output.writeInt(-1)  // Write this magic number to signify end of file
    output.close()
  }

  private def updateFromDeltaFile(
      version: Long,
      maps: mutable.Map[String, HDFSBackedStateStoreMap],
      metadataMap: Map[String, ColFamilyMetadata]): Unit = {

    metadataMap.foreach { case (cfName, metadata) =>
       val fileToRead = deltaFile(version, cfName)
       val map = maps.getOrElseUpdate(cfName, {
          val verifier = KeyValueIntegrityVerifier.create(
            stateStoreId_.toString,
            storeConf.rowChecksumEnabled,
            storeConf.rowChecksumReadVerificationRatio)
          HDFSBackedStateStoreMap.create(metadata.keySchema, metadata.numColsPrefixKey, verifier)
       })
       updateFromDeltaFileForCF(version, fileToRead, map,
         metadata.keySchema, metadata.valueSchema)
    }
  }

  private def updateFromDeltaFileForCF(
      version: Long,
      fileToRead: Path,
      map: HDFSBackedStateStoreMap,
      keySchema: StructType,
      valueSchema: StructType): Unit = {

    var input: DataInputStream = null
    val sourceStream = try {
      fm.open(fileToRead)
    } catch {
      case f: FileNotFoundException =>
        throw QueryExecutionErrors.failedToReadDeltaFileNotExistsError(fileToRead, toString(), f)
    }
    try {
      input = decompressStream(sourceStream)
      var eof = false

      // If row checksum is enabled, verify every record in the file to detect corrupt rows.
      val verifier = KeyValueIntegrityVerifier
        .create(stateStoreId_.toString, storeConf.rowChecksumEnabled, verificationRatio = 1)

      while (!eof) {
        val keySize = input.readInt()
        if (keySize == -1) {
          eof = true
        } else if (keySize < 0) {
          throw QueryExecutionErrors.failedToReadDeltaFileKeySizeError(
            fileToRead, toString(), keySize)
        } else {
          val keyRowBuffer = new Array[Byte](keySize)
          Utils.readFully(input, keyRowBuffer, 0, keySize)

          val keyRow = new UnsafeRow(keySchema.fields.length)

          val valueSize = input.readInt()
          if (valueSize < 0) {
            val originalKeyBytes = if (storeConf.rowChecksumEnabled) {
              // For deleted row, we added checksum to the key side.
              // Decode the original key and remove the checksum part
              KeyValueChecksumEncoder.decodeAndVerifyKeyRowWithChecksum(verifier, keyRowBuffer)
            } else {
              keyRowBuffer
            }
            keyRow.pointTo(originalKeyBytes, originalKeyBytes.length)
            map.remove(keyRow)
          } else {
            keyRow.pointTo(keyRowBuffer, keySize)

            val valueRowBuffer = new Array[Byte](valueSize)
            Utils.readFully(input, valueRowBuffer, 0, valueSize)
            val valueRow = new UnsafeRow(valueSchema.fields.length)

            val (originalValueBytes, valueWrapper) = if (storeConf.rowChecksumEnabled) {
              // checksum is on the value side
              val (valueBytes, checksum) = KeyValueChecksumEncoder
                .decodeSingleValueRowWithChecksum(valueRowBuffer)
              verifier.foreach(_.verify(keyRowBuffer, Some(valueBytes), checksum))
              (valueBytes, StateStoreRowWithChecksum(valueRow, checksum))
            } else {
              (valueRowBuffer, StateStoreRow(valueRow))
            }

            // If valueSize in existing file is not multiple of 8, floor it to multiple of 8.
            // This is a workaround for the following:
            // Prior to Spark 2.3 mistakenly append 4 bytes to the value row in
            // `RowBasedKeyValueBatch`, which gets persisted into the checkpoint data
            valueRow.pointTo(originalValueBytes, (originalValueBytes.length / 8) * 8)
            if (!isValidated && !useColumnFamilies) {
              StateStoreProvider.validateStateRowFormat(
                keyRow, keySchema, valueRow, valueSchema, stateStoreId, storeConf)
              isValidated = true
            }
            map.put(keyRow, valueWrapper)
          }
        }
      }
    } finally {
      if (input != null) input.close()
    }
    logInfo(log"Read delta file for version ${MDC(LogKeys.FILE_VERSION, version)} " +
      log"of ${MDC(LogKeys.STATE_STORE_PROVIDER, this)} from " +
      log"${MDC(LogKeys.FILE_NAME, fileToRead)}")
  }

  private def writeSnapshotFile(
      version: Long,
      map: HDFSBackedStateStoreMap,
      opType: String,
      colFamilyName: String): Unit = {
    val targetFile = snapshotFile(version, colFamilyName)
    var rawOutput: CancellableFSDataOutputStream = null
    var output: DataOutputStream = null
    try {
      rawOutput = fm.createAtomic(targetFile, overwriteIfPossible = true)
      output = compressStream(rawOutput)
      // Entry iterator doesn't do verification, we will do it ourselves
      // Using this instead of iterator() since we want the UnsafeRowWrapper
      val iter = map.entryIterator()

      // If row checksum is enabled, we will verify every entry in the map before writing snapshot,
      // to prevent writing corrupt rows since the map might have been created a while ago.
      // This is fine since write snapshot is typically done in the background.
      val verifier = KeyValueIntegrityVerifier
        .create(stateStoreId_.toString, storeConf.rowChecksumEnabled, verificationRatio = 1)

      while (iter.hasNext) {
        val entry = iter.next()
        verifier.foreach(_.verify(entry.getKey, entry.getValue))

        val keyBytes = entry.getKey.getBytes()
        val valueBytes = entry.getValue match {
          case v: StateStoreRowWithChecksum =>
            // If it has checksum, encode it with the checksum.
            KeyValueChecksumEncoder.encodeSingleValueRowWithChecksum(
              v.unsafeRow().getBytes(), v.checksum)
          case o => o.unsafeRow().getBytes()
        }
        output.writeInt(keyBytes.size)
        output.write(keyBytes)
        output.writeInt(valueBytes.size)
        output.write(valueBytes)
      }
      output.writeInt(-1)
      output.close()
    } catch {
      case e: Throwable =>
        cancelDeltaFile(compressedStream = output, rawStream = rawOutput)
        throw e
    }
    logInfo(log"Written snapshot file for version ${MDC(LogKeys.FILE_VERSION, version)} of " +
      log"${MDC(LogKeys.STATE_STORE_PROVIDER, this)} at ${MDC(LogKeys.FILE_NAME, targetFile)} " +
      log"for ${MDC(LogKeys.OP_TYPE, opType)}")
    // Compare and update with the version that was just uploaded.
    lastUploadedSnapshotVersion.updateAndGet(v => Math.max(version, v))
    // Report the snapshot upload event to the coordinator
    reportSnapshotUploadToCoordinator(version)
  }

  /**
   * Try to cancel the underlying stream and safely close the compressed stream.
   *
   * @param compressedStream the compressed stream.
   * @param rawStream the underlying stream which needs to be cancelled.
   */
  private def cancelDeltaFile(
      compressedStream: DataOutputStream,
      rawStream: CancellableFSDataOutputStream): Unit = {
    try {
      if (rawStream != null) rawStream.cancel()
      Utils.closeQuietly(compressedStream)
    } catch {
      // Closing the compressedStream causes the stream to write/flush flush data into the
      // rawStream. Since the rawStream is already closed, there may be errors.
      // Usually its an IOException. However, Hadoop's RawLocalFileSystem wraps
      // IOException into FSError.
      case e: FSError if e.getCause.isInstanceOf[IOException] =>

      // SPARK-42668 - Catch and log any other exception thrown while trying to cancel
      // raw stream or close compressed stream.
      case NonFatal(ex) =>
        logInfo(log"Failed to cancel delta file for " +
          log"provider=${MDC(LogKeys.STATE_STORE_ID, stateStoreId)} " +
          log"with exception=${MDC(LogKeys.ERROR, ex)}")
    }
  }

  /**
   * Try to read the snapshot file. If the snapshot file is not available, return [[None]].
   *
   * @param version the version of the snapshot file
  */
  private def readSnapshotFile(
      version: Long,
      metadata: ColFamilyMetadata,
      colFamilyName: String): Option[HDFSBackedStateStoreMap] = {
    val fileToRead = snapshotFile(version, colFamilyName)
    val readVerifier = KeyValueIntegrityVerifier.create(
      stateStoreId_.toString,
      storeConf.rowChecksumEnabled,
      storeConf.rowChecksumReadVerificationRatio)
    val map = HDFSBackedStateStoreMap.create(metadata.keySchema, metadata.numColsPrefixKey,
      readVerifier)
    var input: DataInputStream = null

    try {
      input = decompressStream(fm.open(fileToRead))
      var eof = false

      // If row checksum is enabled, verify every record in the file to detect corrupt rows.
      val verifier = KeyValueIntegrityVerifier
        .create(stateStoreId_.toString, storeConf.rowChecksumEnabled, verificationRatio = 1)

      while (!eof) {
        val keySize = input.readInt()
        if (keySize == -1) {
          eof = true
        } else if (keySize < 0) {
          throw QueryExecutionErrors.failedToReadSnapshotFileKeySizeError(
            fileToRead, toString(), keySize)
        } else {
          val keyRowBuffer = new Array[Byte](keySize)
          Utils.readFully(input, keyRowBuffer, 0, keySize)

          val keyRow = new UnsafeRow(metadata.keySchema.fields.length)
          keyRow.pointTo(keyRowBuffer, keySize)

          val valueSize = input.readInt()
          if (valueSize < 0) {
            throw QueryExecutionErrors.failedToReadSnapshotFileValueSizeError(
              fileToRead, toString(), valueSize)
          } else {
            val valueRowBuffer = new Array[Byte](valueSize)
            Utils.readFully(input, valueRowBuffer, 0, valueSize)
            val valueRow = new UnsafeRow(metadata.valueSchema.fields.length)

            val (originalValueBytes, valueWrapper) = if (storeConf.rowChecksumEnabled) {
              // Checksum is on the value side
              val (valueBytes, checksum) = KeyValueChecksumEncoder
                .decodeSingleValueRowWithChecksum(valueRowBuffer)
              verifier.foreach(_.verify(keyRowBuffer, Some(valueBytes), checksum))
              (valueBytes, StateStoreRowWithChecksum(valueRow, checksum))
            } else {
              (valueRowBuffer, StateStoreRow(valueRow))
            }

            // If valueSize in existing file is not multiple of 8, floor it to multiple of 8.
            // This is a workaround for the following:
            // Prior to Spark 2.3 mistakenly append 4 bytes to the value row in
            // `RowBasedKeyValueBatch`, which gets persisted into the checkpoint data
            valueRow.pointTo(originalValueBytes, (originalValueBytes.length / 8) * 8)
            if (!isValidated && !useColumnFamilies) {
              StateStoreProvider.validateStateRowFormat(
                keyRow, metadata.keySchema, valueRow, metadata.valueSchema,
                stateStoreId, storeConf)
              isValidated = true
            }
            map.put(keyRow, valueWrapper)
          }
        }
      }
      logInfo(log"Read snapshot file for version ${MDC(LogKeys.SNAPSHOT_VERSION, version)} " +
        log"of ${MDC(LogKeys.STATE_STORE_PROVIDER, this)} from " +
        log"${MDC(LogKeys.FILE_NAME, fileToRead)}")
      Some(map)
    } catch {
      case _: FileNotFoundException =>
        None
    } finally {
      if (input != null) input.close()
    }
  }


  /** Perform a snapshot of the store to allow delta files to be consolidated */
  private def doSnapshot(opType: String, throwEx: Boolean = false): Unit = {
    try {
      val ((files, _), e1) = Utils.timeTakenMs(fetchFiles())
      logDebug(s"fetchFiles() took $e1 ms.")

      if (files.nonEmpty) {
        val lastVersion = files.last.version
        val deltaFilesForLastVersion =
          filesForVersion(files, lastVersion).filter(_.isSnapshot == false)
        synchronized { Option(loadedMaps.get(lastVersion)) } match {
          case Some(entry) =>
            if (deltaFilesForLastVersion.size > storeConf.minDeltasForSnapshot) {
              val (_, e2) = Utils.timeTakenMs {
                 entry.maps.foreach { case (cfName, map) =>
                   writeSnapshotFile(lastVersion, map, opType, cfName)
                 }
              }
              logDebug(s"writeSnapshotFile() took $e2 ms.")
            }
          case None =>
            // The last map is not loaded, probably some other instance is in charge
        }
      }
    } catch {
      case NonFatal(e) =>
        logWarning(log"Error doing snapshots", e)
        if (throwEx) {
          throw e
        }
    }
  }

  /**
   * Clean up old snapshots and delta files that are not needed any more. It ensures that last
   * few versions of the store can be recovered from the files, so re-executed RDD operations
   * can re-apply updates on the past versions of the store.
   */
  private[state] def cleanup(): Unit = {
    try {
      val ((files, checksumFiles), e1) = Utils.timeTakenMs(fetchFiles())
      logDebug(s"fetchFiles() took $e1 ms.")

      if (files.nonEmpty) {
        val earliestVersionToRetain = files.last.version - storeConf.minVersionsToRetain
        if (earliestVersionToRetain > 0) {
          val earliestFileToRetain = filesForVersion(files, earliestVersionToRetain).head
          val filesToDelete = files.filter(_.version < earliestFileToRetain.version)
          val (_, e2) = Utils.timeTakenMs {
            filesToDelete.foreach { f =>
              fm.delete(f.path)
            }
          }
          logDebug(s"deleting files took $e2 ms.")
          logInfo(log"Deleted files older than " +
            log"${MDC(LogKeys.FILE_VERSION, earliestFileToRetain.version)} for " +
            log"${MDC(LogKeys.STATE_STORE_PROVIDER, this)}: " +
            log"${MDC(LogKeys.FILE_NAME, filesToDelete.mkString(", "))}")

          // Do this only if we see checksum files in the initial dir listing
          // To avoid checking for orphan checksum files, if there is no checksum files
          // (e.g. file checksum was never enabled)
          if (checksumFiles.nonEmpty) {
            StateStoreProvider.deleteOrphanChecksumFiles(
              fm,
              checksumFiles,
              // using empty here because the state files were deleted using fs above
              // (for parallel delete) instead of fm. Hence, checksum files were not
              // deleted and will need to be deleted here.
              deletedStoreFiles = Seq.empty,
              earliestFileToRetain.version,
              storeConf.checkpointFileChecksumEnabled
            )
          }
        }
      }
    } catch {
      case NonFatal(e) =>
        logWarning(log"Error cleaning up files", e)
    }
  }

  /** Files needed to recover the given version of the store */
  private def filesForVersion(allFiles: Seq[StoreFile], version: Long): Seq[StoreFile] = {
    require(version >= 0)
    require(allFiles.exists(_.version == version))

    val latestSnapshotFileBeforeVersion = allFiles
      .filter(_.isSnapshot)
      .takeWhile(_.version <= version)
      .lastOption
    val deltaBatchFiles = latestSnapshotFileBeforeVersion match {
      case Some(snapshotFile) =>

        val deltaFiles = allFiles.filter { file =>
          file.version > snapshotFile.version && file.version <= version
        }.toList

        deltaFiles

      case None =>
        allFiles.takeWhile(_.version <= version)
    }
    latestSnapshotFileBeforeVersion.toSeq ++ deltaBatchFiles
  }

  /** Fetch all the files that back the store */
  private def fetchFiles(): (Seq[StoreFile], Seq[ChecksumFile]) = {
    val files: Seq[FileStatus] = try {
      fm.list(baseDir).toImmutableArraySeq
    } catch {
      case _: java.io.FileNotFoundException =>
        Seq.empty
    }
    val versionToFiles = new mutable.ArrayBuffer[StoreFile]
    files.foreach { status =>
      val path = status.getPath
      val nameParts = path.getName.split("\\.")

      if (nameParts.size >= 2) {
        val versionStr = nameParts(0)
        // Ensure version is numeric
        if (versionStr.nonEmpty && versionStr.forall(_.isDigit)) {
           val version = versionStr.toLong
           val ext = nameParts.last.toLowerCase(Locale.ROOT)

           ext match {
             case "delta" =>
                versionToFiles.append(StoreFile(version, path, isSnapshot = false))
             case "snapshot" =>
                versionToFiles.append(StoreFile(version, path, isSnapshot = true))
             case "metadata" =>
                versionToFiles.append(StoreFile(version, path, isSnapshot = false))
             case _ =>
                // Ignore other files
           }
        }
      }
    }
    val storeFiles = versionToFiles.toSeq.sortBy(_.version)
    val checksumFiles = files
      .filter(f => ChecksumCheckpointFileManager.isChecksumFile(f.getPath))
      .map(f => ChecksumFile(f.getPath))
    logDebug(s"Current set of files for $this: ${storeFiles.mkString(", ")}, " +
      s"checksum files: ${checksumFiles.mkString(", ")}")

    (storeFiles, checksumFiles)
  }

  // Visible to state pkg for testing.
  private[state] def compressStream(outputStream: DataOutputStream): DataOutputStream = {
    val compressed = CompressionCodec.createCodec(sparkConf, storeConf.compressionCodec)
      .compressedOutputStream(outputStream)
    new DataOutputStream(compressed)
  }

  // Visible to state pkg for testing.
  private[state] def decompressStream(inputStream: DataInputStream): DataInputStream = {
    val compressed = CompressionCodec.createCodec(sparkConf, storeConf.compressionCodec)
      .compressedInputStream(inputStream)
    new DataInputStream(compressed)
  }

  private def deltaFile(
      version: Long,
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Path = {
    if (colFamilyName == StateStore.DEFAULT_COL_FAMILY_NAME) {
       new Path(baseDir, s"$version.delta")
    } else {
       new Path(baseDir, s"$version.$colFamilyName.delta")
    }
  }

  private def snapshotFile(
      version: Long,
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Path = {
    if (colFamilyName == StateStore.DEFAULT_COL_FAMILY_NAME) {
       new Path(baseDir, s"$version.snapshot")
    } else {
       new Path(baseDir, s"$version.$colFamilyName.snapshot")
    }
  }

  private def metadataFile(version: Long): Path = {
    new Path(baseDir, s"$version.metadata")
  }

  private def verify(condition: => Boolean, msg: String): Unit = {
    if (!condition) {
      throw new IllegalStateException(msg)
    }
  }

  // Metadata read/write helpers
  private def writeMetadata(version: Long, metadata: Map[String, ColFamilyMetadata]): Unit = {
    if (metadata.isEmpty && !useColumnFamilies) return

    val targetFile = metadataFile(version)
    var rawOutput: CancellableFSDataOutputStream = null
    try {
      rawOutput = fm.createAtomic(targetFile, overwriteIfPossible = true)

      // Serialize to JSON
      implicit val formats: Formats = DefaultFormats
      val json = compact(render(
         JObject("colFamilies" -> JArray(metadata.values.map { m =>
            JObject(
              "colFamilyName" -> JString(m.colFamilyName),
              "keySchema" -> parse(m.keySchema.json),
              "valueSchema" -> parse(m.valueSchema.json),
              "keyStateEncoderSpec" -> m.keyStateEncoderSpec.jsonValue,
              "numColsPrefixKey" -> org.json4s.JInt(m.numColsPrefixKey)
            )
         }.toList))
      ))

      rawOutput.writeBytes(json)
      rawOutput.close()
    } catch {
       case e: Throwable =>
          if (rawOutput != null) rawOutput.cancel()
          throw e
    }
  }

  private def readMetadata(version: Long): Option[Map[String, ColFamilyMetadata]] = {
     val fileToRead = metadataFile(version)
     if (!fm.exists(fileToRead)) return None

     var input: FSDataInputStream = null
     try {
       input = fm.open(fileToRead)
       val jsonStr = scala.io.Source.fromInputStream(input).mkString
       implicit val formats: Formats = DefaultFormats
       val json = parse(jsonStr)

       val colFamilies = (json \ "colFamilies").extract[List[JValue]]
       val metadataMap = colFamilies.map { j =>
          val name = (j \ "colFamilyName").extract[String]
          val keySchema = DataType.fromJson(compact(render(j \ "keySchema")))
            .asInstanceOf[StructType]
          val valueSchema = DataType.fromJson(compact(render(j \ "valueSchema")))
            .asInstanceOf[StructType]
          val encoderSpec = KeyStateEncoderSpec.fromJson(keySchema,
            (j \ "keyStateEncoderSpec").extract[Map[String, Any]])
          val numColsPrefixKey = (j \ "numColsPrefixKey").extract[Int]

          name -> ColFamilyMetadata(name, keySchema, valueSchema, encoderSpec, numColsPrefixKey)
       }.toMap

       Some(metadataMap)
     } catch {
       case e: Throwable =>
         // Log warning?
         logWarning(s"Failed to read metadata for version $version", e)
         None
     } finally {
       if (input != null) input.close()
     }
  }

  /**
   * Get the state store of endVersion by applying delta files on the snapshot of snapshotVersion.
   * If snapshot for snapshotVersion does not exist, an error will be thrown.
   *
   * @param snapshotVersion checkpoint version of the snapshot to start with
   * @param endVersion   checkpoint version to end with
   * @param readOnly whether the state store should be read-only
   * @param snapshotVersionStateStoreCkptId state store checkpoint ID of the snapshot version
   * @param endVersionStateStoreCkptId state store checkpoint ID of the end version
   * @return [[HDFSBackedStateStore]]
   */
  override def replayStateFromSnapshot(
      snapshotVersion: Long,
      endVersion: Long,
      readOnly: Boolean,
      snapshotVersionStateStoreCkptId: Option[String] = None,
      endVersionStateStoreCkptId: Option[String] = None): StateStore = {
    if (snapshotVersionStateStoreCkptId.isDefined || endVersionStateStoreCkptId.isDefined) {
      throw StateStoreErrors.stateStoreCheckpointIdsNotSupported(
        "HDFSBackedStateStoreProvider does not support checkpointFormatVersion > 1 " +
        "but a state store checkpointID is passed in")
    }
    val (newMaps, metadata) = replayLoadedMapFromSnapshot(snapshotVersion, endVersion)
    logInfo(log"Retrieved snapshot at version " +
      log"${MDC(LogKeys.STATE_STORE_VERSION, snapshotVersion)} and apply delta files to version " +
      log"${MDC(LogKeys.STATE_STORE_VERSION, endVersion)} of " +
      log"${MDC(LogKeys.STATE_STORE_PROVIDER, HDFSBackedStateStoreProvider.this)} for update")
    new HDFSBackedStateStore(endVersion, newMaps, metadata)
  }

  /**
   * Get the state store of endVersion for reading by applying delta files on the snapshot of
   * snapshotVersion. If snapshot for snapshotVersion does not exist, an error will be thrown.
   *
   * @param snapshotVersion checkpoint version of the snapshot to start with
   * @param endVersion   checkpoint version to end with
   * @param snapshotVersionStateStoreCkptId state store checkpoint ID of the snapshot version
   * @param endVersionStateStoreCkptId state store checkpoint ID of the end version
   * @return [[HDFSBackedReadStateStore]]
   */
  override def replayReadStateFromSnapshot(
      snapshotVersion: Long,
      endVersion: Long,
      snapshotVersionStateStoreCkptId: Option[String] = None,
      endVersionStateStoreCkptId: Option[String] = None):
    ReadStateStore = {
    if (snapshotVersionStateStoreCkptId.isDefined || endVersionStateStoreCkptId.isDefined) {
      throw StateStoreErrors.stateStoreCheckpointIdsNotSupported(
        "HDFSBackedStateStoreProvider does not support checkpointFormatVersion > 1 " +
        "but a state store checkpointID is passed in")
    }
    val (newMaps, _) = replayLoadedMapFromSnapshot(snapshotVersion, endVersion)
    logInfo(log"Retrieved snapshot at version " +
      log"${MDC(LogKeys.STATE_STORE_VERSION, snapshotVersion)} and apply delta files to version " +
      log"${MDC(LogKeys.STATE_STORE_VERSION, endVersion)} of " +
      log"${MDC(LogKeys.STATE_STORE_PROVIDER, HDFSBackedStateStoreProvider.this)} for read-only")
    new HDFSBackedReadStateStore(endVersion, newMaps.toMap)
  }

  /**
   * Construct the state map at endVersion from snapshot of version snapshotVersion.
   * Returns a new [[HDFSBackedStateStoreMap]]
   * @param snapshotVersion checkpoint version of the snapshot to start with
   * @param endVersion   checkpoint version to end with
   */
  private def replayLoadedMapFromSnapshot(snapshotVersion: Long, endVersion: Long):
  (mutable.Map[String, HDFSBackedStateStoreMap], mutable.Map[String, ColFamilyMetadata]) =
    synchronized {
    try {
      if (snapshotVersion < 1) {
        throw QueryExecutionErrors.unexpectedStateStoreVersion(snapshotVersion)
      }
      if (endVersion < snapshotVersion) {
        throw QueryExecutionErrors.unexpectedStateStoreVersion(endVersion)
      }

      val newMaps = mutable.Map[String, HDFSBackedStateStoreMap]()
      val (loadedMaps, loadedMetadata) = constructMapFromSnapshot(snapshotVersion, endVersion)

      newMaps ++= loadedMaps
      val newMetadata = mutable.Map[String, ColFamilyMetadata]()
      newMetadata ++= loadedMetadata

      (newMaps, newMetadata)
    }
    catch {
      case e: OutOfMemoryError =>
        throw QueryExecutionErrors.notEnoughMemoryToLoadStore(
          stateStoreId.toString,
          "HDFS_STORE_PROVIDER",
          e)
      case e: Throwable => throw StateStoreErrors.cannotLoadStore(e)
    }
  }

  private def constructMapFromSnapshot(snapshotVersion: Long, endVersion: Long):
  (Map[String, HDFSBackedStateStoreMap], Map[String, ColFamilyMetadata]) = {
    val (result, elapsedMs) = Utils.timeTakenMs {
      val loadedEntry = synchronized { Option(loadedMaps.get(snapshotVersion)) }
      val (startVersionMaps, startVersionMetadata) = loadedEntry match {
        case Some(entry) =>
            // Explicit cast to avoid existential type issues
            (Some(entry.maps.asInstanceOf[Map[String, HDFSBackedStateStoreMap]]),
             Some(entry.metadata.asInstanceOf[Map[String, ColFamilyMetadata]]))
        case None =>
             val metadataOpt = readMetadata(snapshotVersion).orElse {
                 // Fallback
                 Some(Map(StateStore.DEFAULT_COL_FAMILY_NAME ->
                   ColFamilyMetadata(StateStore.DEFAULT_COL_FAMILY_NAME, keySchema, valueSchema,
                     keyStateEncoderSpec, numColsPrefixKey)))
             }

             if (metadataOpt.isEmpty) {
                (None, None)
             } else {
                 val metadata = metadataOpt.get
                 val maps = mutable.Map[String, HDFSBackedStateStoreMap]()
                 var anySnapshotFound = false
                 metadata.foreach { case (cfName, m) =>
                       readSnapshotFile(snapshotVersion, m, cfName).foreach { map =>
                          maps.put(cfName, map)
                          anySnapshotFound = true
                       }
                 }

                 if (maps.isEmpty && metadata.nonEmpty && snapshotVersion > 0) {
                     (None, Some(metadata))
                 } else {
                     (Some(maps.toMap), Some(metadata))
                 }
             }
      }

      if (startVersionMaps.isEmpty) {
        throw StateStoreErrors.stateStoreSnapshotFileNotFound(
          snapshotFile(snapshotVersion).toString, toString())
      }

      // Load all the deltas from the version after the start version up to the end version.
      val resultMaps = mutable.Map[String, HDFSBackedStateStoreMap]()

      val maps: Map[String, HDFSBackedStateStoreMap] = startVersionMaps.get
      val meta: Map[String, ColFamilyMetadata] = startVersionMetadata.get

      // Copy initial
      maps.foreach { case (cf, map) =>
         val verifier = KeyValueIntegrityVerifier.create(
              stateStoreId_.toString,
              storeConf.rowChecksumEnabled,
              storeConf.rowChecksumReadVerificationRatio)
         val m = meta.get(cf).get
         val newMap = HDFSBackedStateStoreMap.create(m.keySchema, m.numColsPrefixKey,
           verifier)
         newMap.putAll(map)
         resultMaps.put(cf, newMap)
      }

      val endMetadata = readMetadata(endVersion).getOrElse(meta)

      for (deltaVersion <- snapshotVersion + 1 to endVersion) {
         updateFromDeltaFile(deltaVersion, resultMaps, endMetadata)
      }

      (resultMaps.toMap, endMetadata)
    }

    logDebug(s"Loading snapshot at version $snapshotVersion and apply delta files to version " +
      s"$endVersion takes $elapsedMs ms.")

    result
  }

  override def getStateStoreChangeDataReader(
      startVersion: Long,
      endVersion: Long,
      colFamilyNameOpt: Option[String] = None,
      endVersionStateStoreCkptId: Option[String] = None):
    StateStoreChangeDataReader = {

    if (endVersionStateStoreCkptId.isDefined) {
      throw StateStoreErrors.stateStoreCheckpointIdsNotSupported(
        "HDFSBackedStateStoreProvider does not support checkpointFormatVersion > 1 " +
        "but a state store checkpointID is passed in")
    }

    if (colFamilyNameOpt.isDefined && !useColumnFamilies
      && colFamilyNameOpt.get != StateStore.DEFAULT_COL_FAMILY_NAME) {
       throw StateStoreErrors.multipleColumnFamiliesNotSupported(providerName)
    }

    new HDFSBackedStateStoreChangeDataReader(stateStoreId_, fm, baseDir, startVersion, endVersion,
      CompressionCodec.createCodec(sparkConf, storeConf.compressionCodec),
      keySchema, valueSchema, storeConf, colFamilyNameOpt)
  }

  /** Reports to the coordinator the store's latest snapshot version */
  private def reportSnapshotUploadToCoordinator(version: Long): Unit = {
    if (storeConf.reportSnapshotUploadLag) {
      val currentTimestamp = System.currentTimeMillis()
      StateStoreProvider.coordinatorRef.foreach(
        _.snapshotUploaded(stateStoreProviderId, version, currentTimestamp)
      )
    }
  }
}

object HDFSBackedStateStoreProvider {
  case class LoadedMapEntry(
      maps: Map[String, HDFSBackedStateStoreMap],
      metadata: Map[String, ColFamilyMetadata])
}

/** [[StateStoreChangeDataReader]] implementation for [[HDFSBackedStateStoreProvider]] */
class HDFSBackedStateStoreChangeDataReader(
    storeId: StateStoreId,
    fm: CheckpointFileManager,
    stateLocation: Path,
    startVersion: Long,
    endVersion: Long,
    compressionCodec: CompressionCodec,
    keySchema: StructType,
    valueSchema: StructType,
    storeConf: StateStoreConf,
    colFamilyNameOpt: Option[String] = None)
  extends StateStoreChangeDataReader(
    storeId, fm, stateLocation, startVersion, endVersion, compressionCodec,
    storeConf, colFamilyNameOpt) {

  override protected val changelogSuffix: String = "delta"

  private var myReader: HDFSBackedStateStoreChangelogReader = _
  private var _currentChangelogVersion: Long = startVersion - 1

  override protected def currentChangelogVersion: Long = _currentChangelogVersion

  override def getNext(): (RecordType.Value, UnsafeRow, UnsafeRow, Long) = {
    if (startVersion > endVersion) return null

    // Manage version iteration
    if (myReader == null || !myReader.hasNext) {
       if (myReader != null) {
         myReader.close()
         myReader = null
         _currentChangelogVersion += 1
       } else {
         _currentChangelogVersion = startVersion
       }

       if (_currentChangelogVersion > endVersion) return null

       val cfName = colFamilyNameOpt.getOrElse(StateStore.DEFAULT_COL_FAMILY_NAME)
       val path = if (cfName == StateStore.DEFAULT_COL_FAMILY_NAME) {
          new Path(stateLocation, s"${_currentChangelogVersion}.delta")
       } else {
          new Path(stateLocation, s"${_currentChangelogVersion}.$cfName.delta")
       }

       if (fm.exists(path)) {
          val sourceStream = fm.open(path)
          val input = CompressionCodec.createCodec(
            Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf), storeConf.compressionCodec)
            .compressedInputStream(sourceStream)
          myReader = new HDFSBackedStateStoreChangelogReader(new DataInputStream(input))
       } else {
          if (cfName == StateStore.DEFAULT_COL_FAMILY_NAME) {
             throw QueryExecutionErrors.failedToReadDeltaFileNotExistsError(
               path, toString(), new FileNotFoundException())
          }
          return getNext()
       }
    }

    val (recordType, keyArray, valueArray) = myReader.next()

    val keyRow = new UnsafeRow(keySchema.fields.length)
    if (valueArray == null) {
      val originalKeyBytes = if (storeConf.rowChecksumEnabled) {
        // Decode the original key and remove the checksum part
        KeyValueChecksumEncoder.decodeAndVerifyKeyRowWithChecksum(readVerifier, keyArray)
      } else {
        keyArray
      }
      keyRow.pointTo(originalKeyBytes, originalKeyBytes.length)
      (recordType, keyRow, null, _currentChangelogVersion)
    } else {
      keyRow.pointTo(keyArray, keyArray.length)

      val valueRow = new UnsafeRow(valueSchema.fields.length)
      val originalValueBytes = if (storeConf.rowChecksumEnabled) {
        // Checksum is on the value side
        KeyValueChecksumEncoder.decodeAndVerifySingleValueRowWithChecksum(
          readVerifier, keyArray, valueArray)
      } else {
        valueArray
      }

      // If valueSize in existing file is not multiple of 8, floor it to multiple of 8.
      // This is a workaround for the following:
      // Prior to Spark 2.3 mistakenly append 4 bytes to the value row in
      // `RowBasedKeyValueBatch`, which gets persisted into the checkpoint data
      valueRow.pointTo(originalValueBytes, (originalValueBytes.length / 8) * 8)
      (recordType, keyRow, valueRow, _currentChangelogVersion)
    }
  }

  override def close(): Unit = {
    if (myReader != null) {
      myReader.close()
      myReader = null
    }
    super.close()
  }
}

// Helper class for reading delta file
// (borrowed logic from HDFSBackedStateStoreChangeDataReader/Provider)
class HDFSBackedStateStoreChangelogReader(input: DataInputStream)
  extends Iterator[(RecordType.Value, Array[Byte], Array[Byte])] with Closeable {
  private var nextRecord: (RecordType.Value, Array[Byte], Array[Byte]) = _
  private var eof = false

  advance()

  private def advance(): Unit = {
    try {
      val keySize = input.readInt()
      if (keySize == -1) {
        eof = true
        nextRecord = null
      } else if (keySize < 0) {
        throw new IOException(s"Invalid key size: $keySize")
      } else {
        val keyRowBuffer = new Array[Byte](keySize)
        Utils.readFully(input, keyRowBuffer, 0, keySize)

        val valueSize = input.readInt()
        if (valueSize < 0) {
           // Delete
           nextRecord = (RecordType.DELETE_RECORD, keyRowBuffer, null)
        } else {
           // Put
           val valueRowBuffer = new Array[Byte](valueSize)
           Utils.readFully(input, valueRowBuffer, 0, valueSize)
           nextRecord = (RecordType.PUT_RECORD, keyRowBuffer, valueRowBuffer)
        }
      }
    } catch {
      case _: EOFException =>
        eof = true
        nextRecord = null
    }
  }

  override def hasNext: Boolean = !eof && nextRecord != null

  override def next(): (RecordType.Value, Array[Byte], Array[Byte]) = {
    if (!hasNext) throw new NoSuchElementException
    val res = nextRecord
    advance()
    res
  }

  override def close(): Unit = {
    input.close()
  }
}
