package org.radekbor.join

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.{IntWritable, Text, Writable, WritableComparable}

class CompositeKey(_key: Text, _order: Int) extends Writable with WritableComparable[CompositeKey] {
  def this() = this(new Text(), 0)

  private val _joinKey = _key
  val joinKey: Text = _joinKey
  private val _keyOrder = _order
  val keyOrder: IntWritable = new IntWritable(_keyOrder)

  override def compareTo(taggedKey: CompositeKey): Int = {
    var compareValue = this.joinKey.compareTo(taggedKey.joinKey)
    if (compareValue == 0) compareValue = this.keyOrder.compareTo(taggedKey.keyOrder)
    compareValue
  }

  override def write(out: DataOutput): Unit = {
    this.joinKey.write(out)
    this.keyOrder.write(out)
  }

  override def readFields(in: DataInput): Unit = {
    this.joinKey.readFields(in)
    this.keyOrder.readFields(in)
  }
}
