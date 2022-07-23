package org.apache.spark.sql.cassandra.execution

import scala.collection.mutable
import com.datastax.spark.connector.{TTL, WriteTime}
import com.datastax.spark.connector.datasource.ScanHelper.CqlQueryParts
import com.datastax.spark.connector.datasource.{CassandraCatalog, CassandraScan, CassandraSourceUtil}
import com.datastax.spark.connector.rdd.partitioner.DataSizeEstimates
import com.datastax.spark.connector.rdd.partitioner.dht.TokenFactory
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, ExprId, NamedExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.BATCH_SCAN
import org.apache.spark.sql.columnar.CachedBatchSerializer
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, ReusedBatchScanExec}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra.CassandraSourceRelation.{getPersistBatchScanSetting, getReuseBatchScanSetting}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.columnar.{CachedRDDBuilder, InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

case class CassandraReuseBatchScanStrategy(spark: SparkSession) extends Rule[SparkPlan] {
  import CassandraReuseBatchScanStrategy._

  override def apply(plan: SparkPlan): SparkPlan = {
    if (shouldReuse(plan)) {
      val cachedScans = plan.collectLeaves()
        .filter(canReuse)
        .map(_.asInstanceOf[BatchScanExec])
        .map(batch => {
          val scan = batch.scan.asInstanceOf[CassandraScan]
          (scan.tableDef, scan)
        })
        .groupBy(_._1)
        .filter(_._2.size > 1)
        .map {
          case (table, scans) =>
            (
              table,
              scans
                .map(_._2)
                .reduce((merged, scan) => mergeCassandraScan(merged, scan))
            )
        }
        .map { case (table, scan) => (table, reuseBatchScan(scan)) }

      plan.transformUpWithPruning(_.containsAnyPattern(BATCH_SCAN)) {
        case BatchScanExec(output, scan: CassandraScan, _) if cachedScans.contains(scan.tableDef) =>
          val cachedScan = cachedScans(scan.tableDef)
          val aliases = output.map(ar => {
            val oldAttr = cachedScan.output.find(_.name.equals(ar.name)).getOrElse(ar)
            Alias(oldAttr, ar.name)(ar.exprId)
          })
          ProjectExec(aliases, cachedScan)
      }
    } else {
      plan
    }
  }
}

object CassandraReuseBatchScanStrategy {
  def shouldReuse(plan: SparkPlan): Boolean = {
    getCassandraScan(plan).exists(scan => getReuseBatchScanSetting(scan.consolidatedConf))
  }

  def canReuse(plan: SparkPlan): Boolean = {
    if (isCassandraBatchScan(plan)) {
      val batch = plan.asInstanceOf[BatchScanExec]
      val scan = batch.scan.asInstanceOf[CassandraScan]
      batch.runtimeFilters.isEmpty && isSelectOnlyCqlQueryParts(scan.cqlQueryParts)
    } else {
      false
    }
  }

  def isCassandraBatchScan(plan: SparkPlan): Boolean = {
    plan.isInstanceOf[BatchScanExec] && plan.asInstanceOf[BatchScanExec].scan.isInstanceOf[CassandraScan]
  }

  def isSelectOnlyCqlQueryParts(parts: CqlQueryParts): Boolean = {
    parts.whereClause.predicates.isEmpty && parts.limitClause.isEmpty &&
      parts.clusteringOrder.isEmpty
  }

  def mergeCassandraScan(lhs: CassandraScan, rhs: CassandraScan): CassandraScan = {
    val TTLCapture = "TTL\\((.*)\\)".r
    val WriteTimeCapture = "WRITETIME\\((.*)\\)".r

    val readSchema = StructType.merge(lhs.readSchema, rhs.readSchema).asInstanceOf[StructType]
    val selectedColumns = readSchema.fieldNames.collect {
      case name@TTLCapture(column) => TTL(column, Some(name))
      case name@WriteTimeCapture(column) => WriteTime(column, Some(name))
      case column => lhs.tableDef.columnByName(column).ref
    }

    CassandraScan(
      lhs.session,
      lhs.connector,
      lhs.tableDef,
      CqlQueryParts(selectedColumns),
      readSchema,
      lhs.readConf,
      lhs.consolidatedConf
    )
  }

  def getCassandraScan(plan: SparkPlan): Option[CassandraScan] = {
    plan.collectLeaves().collectFirst { case BatchScanExec(_, cs: CassandraScan, _) => cs }
  }

  def estimateDataSizePerPartitionInBytes(scan: BatchScanExec): Long = {
    import CassandraCatalog._

    val cs = scan.scan.asInstanceOf[CassandraScan]

    implicit val tokenFactory = TokenFactory.forSystemLocalPartitioner(cs.connector)
    val totalSize = new DataSizeEstimates(
      cs.connector,
      cs.tableDef.keyspaceName,
      cs.tableDef.tableName
    ).totalDataSizeInBytes

    val metadata = getTableMetaData(
      cs.connector,
      Identifier.of(Array(cs.tableDef.keyspaceName), cs.tableDef.tableName)
    )

    val structType = CassandraSourceUtil.toStructType(metadata)
    val rowSize: Long = structType.map(_.dataType.defaultSize).sum
    val queryRowSize: Long = cs.readSchema.map(_.dataType.defaultSize).sum

    queryRowSize * totalSize / rowSize / cs.outputPartitioning().numPartitions()
  }

  def reuseBatchScan(cs: CassandraScan): SparkPlan = {
    val scan = BatchScanExec(cs.readSchema.toAttributes, cs, Nil)
    val size = estimateDataSizePerPartitionInBytes(scan)
    getPersistBatchScanSetting(cs.consolidatedConf).getStorageLevel(size) match {
      case StorageLevel.NONE => ReusedBatchScanExec(scan.output, scan)
      case storageLevel => cacheBatchScan(scan, storageLevel, Option(cs.tableDef.tableName))
    }
  }

  def cacheBatchScan(
                      scan: BatchScanExec,
                      storageLevel: StorageLevel,
                      tableName: Option[String] = Option.empty
                    ): InMemoryTableScanExec = {
    val cacheBuilder = CachedRDDBuilder(
      getSerializer(scan.conf), storageLevel, scan, tableName)
    val cacheRelation = InMemoryRelation(
      scan.output, cacheBuilder, scan.outputOrdering)
    InMemoryTableScanExec(scan.output, Nil, cacheRelation)
  }

  var ser: Option[CachedBatchSerializer] = None

  def getSerializer(sqlConf: SQLConf): CachedBatchSerializer = synchronized {
    if (ser.isEmpty) {
      val serName = sqlConf.getConf(StaticSQLConf.SPARK_CACHE_SERIALIZER)
      val serClass = Utils.classForName(serName)
      val instance = serClass.getConstructor().newInstance().asInstanceOf[CachedBatchSerializer]
      ser = Some(instance)
    }
    ser.get
  }
}
