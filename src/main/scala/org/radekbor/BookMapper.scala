package org.radekbor

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.json.XML


class BookMapper extends Mapper[LongWritable, Text, Text, LongWritable] {

  import java.io.IOException

  import org.codehaus.jettison.json.JSONException

  @throws[IOException]
  @throws[InterruptedException]
  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, LongWritable]#Context): Unit = {
    val xml_data = value.toString
    try {
      val xml_to_json = XML.toJSONObject(xml_data)
      val value = xml_to_json.getJSONObject("book").getString("name")
      context.write(new Text(value), new LongWritable(0))
    } catch {
      case je: JSONException =>
        System.out.println(je.toString)
    }
  }
}
