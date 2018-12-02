package org.radekbor.xml

import java.io.IOException

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.io.{DataOutputBuffer, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

// TODO needs to be simplify
object XmlInputFormat {

  class XmlRecordReader extends RecordReader[LongWritable, Text] {
    val START_TAG_KEY = "<book>"
    val END_TAG_KEY = "</book>"
    private var startTag = START_TAG_KEY.getBytes("utf-8")
    private var endTag = END_TAG_KEY.getBytes("utf-8")
    private var start = 0L
    private var end = 0L
    private var fsin: FSDataInputStream = _
    private val buffer = new DataOutputBuffer
    private val key = new LongWritable
    private val value = new Text

    @throws[IOException]
    @throws[InterruptedException]
    override def initialize(is: InputSplit, tac: TaskAttemptContext): Unit = {
      val fileSplit = is.asInstanceOf[FileSplit]

      start = fileSplit.getStart
      end = start + fileSplit.getLength
      val file = fileSplit.getPath
      val fs = file.getFileSystem(tac.getConfiguration)
      fsin = fs.open(fileSplit.getPath)
      fsin.seek(start)
    }

    @throws[IOException]
    @throws[InterruptedException]
    override def nextKeyValue: Boolean = {
      if (fsin.getPos < end) if (readUntilMatch(startTag, false)) try {
        buffer.write(startTag)
        if (readUntilMatch(endTag, true)) {
          value.set(buffer.getData, 0, buffer.getLength)
          key.set(fsin.getPos)
          return true
        }
      } finally buffer.reset
      false
    }

    @throws[IOException]
    @throws[InterruptedException]
    override def getCurrentKey: LongWritable = key

    @throws[IOException]
    @throws[InterruptedException]
    override def getCurrentValue: Text = value

    @throws[IOException]
    @throws[InterruptedException]
    override def getProgress: Float = (fsin.getPos - start) / (end - start).toFloat

    @throws[IOException]
    override def close(): Unit = {
      fsin.close()
    }

    @throws[IOException]
    private def readUntilMatch(`match`: Array[Byte], withinBlock: Boolean): Boolean = {
      var i = 0
      while (i >= 0) {
        val b = fsin.read()
        if (b == -1) {
          return false
        }
        if (withinBlock) buffer.write(b)
        if (b == `match`(i)) {
          i += 1
          if (i >= `match`.length) return true
        }
        else i = 0
        if (!withinBlock && i == 0 && fsin.getPos >= end) return false
      }
      return true
    }
  }

}

class XmlInputFormat extends TextInputFormat {
  override def createRecordReader(split: InputSplit, context: TaskAttemptContext) = {
    new XmlInputFormat.XmlRecordReader
  }
}