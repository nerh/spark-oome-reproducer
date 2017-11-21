package org.example.oome;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/*
 * Example application that mimics my "production" app behaviour: it process some amount of data,
 * coalesce it to min possible amount of executors and then caching it with MEMORY_AND_DISK_2 storage level.
 *
 * Executors throws OOME when trying to replicate cached blocks with stack traces like following:
 * 17/11/21 17:37:24 WARN TaskSetManager: Lost task 1.0 in stage 7.0 (TID 44, 172.24.0.4, executor 0): java.lang.OutOfMemoryError: Java heap space
 *       at io.netty.buffer.UnpooledHeapByteBuf.<init>(UnpooledHeapByteBuf.java:45)
 *       at io.netty.buffer.UnpooledUnsafeHeapByteBuf.<init>(UnpooledUnsafeHeapByteBuf.java:29)
 *       at io.netty.buffer.UnpooledByteBufAllocator.newHeapBuffer(UnpooledByteBufAllocator.java:59)
 *       at io.netty.buffer.AbstractByteBufAllocator.heapBuffer(AbstractByteBufAllocator.java:158)
 *       at io.netty.buffer.AbstractByteBufAllocator.heapBuffer(AbstractByteBufAllocator.java:149)
 *       at io.netty.buffer.Unpooled.buffer(Unpooled.java:116)
 *       at org.apache.spark.network.shuffle.protocol.BlockTransferMessage.toByteBuffer(BlockTransferMessage.java:78)
 *       at org.apache.spark.network.netty.NettyBlockTransferService.uploadBlock(NettyBlockTransferService.scala:139)
 *       at org.apache.spark.network.BlockTransferService.uploadBlockSync(BlockTransferService.scala:122)
 *       at org.apache.spark.storage.BlockManager.org$apache$spark$storage$BlockManager$$replicate(BlockManager.scala:1290)
 *       at org.apache.spark.storage.BlockManager$$anonfun$doPutIterator$1.apply(BlockManager.scala:1103)
 *       at org.apache.spark.storage.BlockManager$$anonfun$doPutIterator$1.apply(BlockManager.scala:1029)
 *       at org.apache.spark.storage.BlockManager.doPut(BlockManager.scala:969)
 *       at org.apache.spark.storage.BlockManager.doPutIterator(BlockManager.scala:1029)
 *       at org.apache.spark.storage.BlockManager.getOrElseUpdate(BlockManager.scala:760)
 *       at org.apache.spark.rdd.RDD.getOrCompute(RDD.scala:334)
 *       at org.apache.spark.rdd.RDD.iterator(RDD.scala:285)
 *       at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:38)
 *       at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:323)
 *       at org.apache.spark.rdd.RDD.iterator(RDD.scala:287)
 *       at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:38)
 *       at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:323)
 *       at org.apache.spark.rdd.RDD.iterator(RDD.scala:287)
 *       at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:38)
 *       at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:323)
 *       at org.apache.spark.rdd.RDD.iterator(RDD.scala:287)
 *       at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:96)
 *       at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:53)
 *       at org.apache.spark.scheduler.Task.run(Task.scala:108)
 *       at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:335)
 *       at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
 *       at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
 *
 *  Seems like on successful block storage BlockManager.doPutIterator calls replicate
 *  with "bytesToReplicate" fetched via doGetLocalBytes call which is trying to fetch data from disk (*first copy*).
 *  Then that data is copied again in NettyBlockTransferService.uploadBlock where blockData converted to array (second copy).
 *  And then UploadBlock.toByteBuffer attempting to allocate yet another buffer (let's name it third copy).
 *
 *  Having blocks whose size is about storage memory size that would result in OOME, because:
 *  - by default, storage memory size is about 30% of all JVM's heap size, suppose that the blocks size is about 25% of all heap
 *  - after "first copy" about 50% of the heap will be used
 *  - after "second copy" about 75% of the heap will be used
 *  - at the UploadBlock.toByteBuffer call there is almost no room for yet another huge object.
 */
public class App {
  public static void main(String[] args) throws Exception {
    String sparkMasterUrl = Objects.requireNonNull(System.getenv("SPARK_MASTER_URL"));
    StorageLevel storageLevel = StorageLevel.fromString(System.getenv("STORAGE_LEVEL"));

    // submit 1 executor per worker, run at most 1 task in parallel
    SparkConf conf = new SparkConf()
      .setAppName("oome-reproducer: " + storageLevel)
      .setMaster(sparkMasterUrl)
      .set("spark.task.maxFailures", "2")
      .set("spark.executor.cores", "1")
      .set("spark.cores.max", "2")
      .set("spark.executor.extraJavaOptions", "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/heap-dumps/");

    JavaSparkContext context = new JavaSparkContext(conf);

    JavaRDD<Row> rows = context.parallelize(IntStream.range(0, 350 * 1024).boxed().collect(Collectors.toList()))
      .repartition(20)
      .flatMap((i) -> IntStream.range(0, 3 * 256).boxed().collect(Collectors.toList()).iterator())
      .map(RowFactory::create);
    StructType schema = new StructType().add("i", DataTypes.IntegerType);

    Dataset<Row> table = SparkSession.builder().sparkContext(context.sc()).getOrCreate().createDataFrame(rows, schema);

    table = table.repartition(10).cache();
    table.count();

    table = table.repartition(2).persist(storageLevel);

    System.out.println(table.count());
  }
}
