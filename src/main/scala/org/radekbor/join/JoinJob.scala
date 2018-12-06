package org.radekbor.join

import java.{lang, util}

import com.sun.scenario.effect.Identity
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.mapred.{JobConf, OutputCollector, Reporter}
import org.apache.hadoop.mapred.lib.IdentityMapper
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, KeyValueTextInputFormat, MultipleInputs}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Partitioner, Reducer}
import org.apache.hadoop.util.Tool
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class MyReducer extends Reducer[Text, Text, Text, Text] {

  private val log = LoggerFactory.getLogger(classOf[MyReducer])

  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    val builder = new StringBuilder

    import scala.collection.JavaConversions._
    for (value <- values) {
      builder.append(value.toString).append(",")
    }



    context.write(key, new Text(builder.toString()))
  }

}

class MyPartitioner extends Partitioner[Text, Text] {
  def getPartition(key: Text, text: Text, numPartitions: Int): Int = key.hashCode % numPartitions
}

import org.apache.hadoop.io.WritableComparable
import org.apache.hadoop.io.WritableComparator

class MyComparator extends WritableComparator(classOf[Text], true) {

  override def compare(a: WritableComparable[_], b: WritableComparable[_]): Int = {
    a.asInstanceOf[Text].compareTo(b.asInstanceOf[Text])
  }
}

class JoinJob extends Configured with Tool {
  override def run(args: Array[String]): Int = {
    val configuration = new Configuration

    val job = Job.getInstance(configuration, "City")
    job.setJarByClass(this.getClass)

    MultipleInputs.addInputPath(job, new Path(args(0)), classOf[KeyValueTextInputFormat])
    MultipleInputs.addInputPath(job, new Path(args(1)), classOf[KeyValueTextInputFormat])

    job.setReducerClass(classOf[MyReducer])
    job.setPartitionerClass(classOf[MyPartitioner])
    job.setGroupingComparatorClass(classOf[MyComparator])
    job.setOutputValueClass(classOf[Text])
    job.setOutputKeyClass(classOf[Text])

    FileOutputFormat.setOutputPath(job, new Path(args(2)))
    if (job.waitForCompletion(true)) 0 else 1
  }
}


