package org.radekbor

import org.apache.commons.io.input.XmlStreamReader
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.mapred.lib.IdentityMapper
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.apache.hadoop.streaming._
class ParseJob extends Configured with Tool {
  override def run(args: Array[String]): Int = {
    val configuration = new Configuration

    val job = Job.getInstance(configuration, "Parse books")

    job.setJarByClass(this.getClass)

    job.setInputFormatClass(classOf[XmlInputFormat])
    job.setOutputValueClass(classOf[LongWritable])
    job.setOutputKeyClass(classOf[Text])

    job.setMapperClass(classOf[BookMapper])

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    if (job.waitForCompletion(true)) 0 else 1
  }
}


object ParseJob {
  def main(args: Array[String]): Unit = {
    val result = ToolRunner.run(new ParseJob(), args)
    System.exit(result)
  }

}