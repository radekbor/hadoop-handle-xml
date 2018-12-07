package org.radekbor.join

import java.lang

import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input.{KeyValueTextInputFormat, MultipleInputs}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Partitioner, Reducer}
import org.apache.hadoop.util.Tool
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

class MyReducer extends Reducer[CompositeKey, Text, Text, Text] {

  private val log = LoggerFactory.getLogger(classOf[MyReducer])

  override def reduce(key: CompositeKey, values: lang.Iterable[Text], context: Reducer[CompositeKey, Text, Text, Text]#Context): Unit = {
    import scala.collection.JavaConversions._
    var items = ListBuffer[String]()
    for (value <- values) {
      items += value.toString
    }
    val allItems = items.toList
    context.write(new Text(allItems.head), new Text(allItems.tail mkString ","))
  }

}

class MyPartitioner extends Partitioner[CompositeKey, Text] {
  def getPartition(key: CompositeKey, text: Text, numPartitions: Int): Int = key.hashCode % numPartitions
}

import org.apache.hadoop.io.{WritableComparable, WritableComparator}

// Comparator decide how weather two elements goes to the same reducer
class ReducerComparator extends WritableComparator(classOf[CompositeKey], true) {

  override def compare(a: WritableComparable[_], b: WritableComparable[_]): Int = {
    a.asInstanceOf[CompositeKey].joinKey.compareTo(b.asInstanceOf[CompositeKey].joinKey)
  }
}


class CityMapper extends Mapper[Text, Text, CompositeKey, Text] {

  private val log = LoggerFactory.getLogger(classOf[CityMapper])

  override def map(key: Text, value: Text, context: Mapper[Text, Text, CompositeKey, Text]#Context): Unit = {
    context.write(new CompositeKey(key, 0), new Text(value))
  }
}

class PersonMapper extends Mapper[Text, Text, CompositeKey, Text] {

  private val log = LoggerFactory.getLogger(classOf[PersonMapper])

  override def map(key: Text, value: Text, context: Mapper[Text, Text, CompositeKey, Text]#Context): Unit = {
    context.write(new CompositeKey(key, 1), new Text(value))
  }
}

class JoinJob extends Configured with Tool {
  override def run(args: Array[String]): Int = {
    val configuration = new Configuration

    val job = Job.getInstance(configuration, "City")
    job.setJarByClass(this.getClass)

    MultipleInputs.addInputPath(job, new Path(args(0)), classOf[KeyValueTextInputFormat], classOf[CityMapper])
    MultipleInputs.addInputPath(job, new Path(args(1)), classOf[KeyValueTextInputFormat], classOf[PersonMapper])

    job.setReducerClass(classOf[MyReducer])
    job.setPartitionerClass(classOf[MyPartitioner])
    job.setGroupingComparatorClass(classOf[ReducerComparator])
    job.setOutputValueClass(classOf[Text])
    job.setOutputKeyClass(classOf[CompositeKey])

    FileOutputFormat.setOutputPath(job, new Path(args(2)))
    if (job.waitForCompletion(true)) 0 else 1
  }
}


