package org.radekbor.keyvalue

import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, KeyValueTextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper}
import org.apache.hadoop.util.Tool
import org.slf4j.LoggerFactory


class ClubMapper extends Mapper[Text, Text, LongWritable, Text] {

  private val log = LoggerFactory.getLogger(classOf[ClubMapper])

  override def map(key: Text, value: Text, context: Mapper[Text, Text, LongWritable, Text]#Context): Unit = {
    context.write(new LongWritable(key.toString.toInt), new Text(value))
  }
}


class ParseKVJob extends Configured with Tool {
  override def run(args: Array[String]): Int = {
    val configuration = new Configuration

    val job = Job.getInstance(configuration, "Parse Clubs")
    // TODO FIXME not working
    job.setJarByClass(this.getClass)

    job.setInputFormatClass(classOf[KeyValueTextInputFormat])
    job.setMapperClass(classOf[ClubMapper])

    job.setOutputValueClass(classOf[Text])
    job.setOutputKeyClass(classOf[LongWritable])

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    if (job.waitForCompletion(true)) 0 else 1
  }
}


