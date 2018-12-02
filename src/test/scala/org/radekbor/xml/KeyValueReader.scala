//package org.radekbor.xml
//
//import org.apache.hadoop.conf.Configuration
//import org.radekbor.keyvalue.ParseKVJob
//import org.scalatest.Matchers
//
//class KeyValueReader extends org.scalatest.FunSuite with Matchers {
//
//  test("Should return names") {
//    import org.apache.hadoop.fs.FileSystem
//    val conf = new Configuration()
//    conf.set("fs.default.name", "file:///")
//    conf.set("mapred.job.tracker", "local")
//
//    import org.apache.hadoop.fs.Path
//    val input = new Path("./src/test/resources/key-value")
//    val output = new Path("./target/output/key-value")
//    val fs = FileSystem.getLocal(conf)
//    fs.delete(output, true) // delete old output
//
//    val driver = new ParseKVJob
//    driver.setConf(conf)
//    val exitCode = driver.run(Array[String](input.toString, output.toString))
//    exitCode should be(0)
//
//  }
//
//}
