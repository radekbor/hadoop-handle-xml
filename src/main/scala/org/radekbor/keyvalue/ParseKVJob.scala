package org.radekbor.keyvalue

import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, KeyValueTextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.Tool

class ParseKVJob extends Configured with Tool {
  override def run(args: Array[String]): Int = {
    val configuration = new Configuration

    val job = Job.getInstance(configuration, "Parse Clubs")
    // TODO FIXME not working
    configuration.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",")
    job.setJarByClass(this.getClass)

    job.setInputFormatClass(classOf[KeyValueTextInputFormat])
    job.setOutputValueClass(classOf[Text])
    job.setOutputKeyClass(classOf[Text])


    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    if (job.waitForCompletion(true)) 0 else 1
  }
}


