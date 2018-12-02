package org.radekbor.xml

import org.apache.hadoop.conf.Configuration
import org.scalatest.Matchers

class ParseJobTest extends org.scalatest.FunSuite with Matchers {

  test("Should return names") {
    import org.apache.hadoop.fs.FileSystem
    val conf = new Configuration()
    conf.set("fs.default.name", "file:///")
    conf.set("mapred.job.tracker", "local")
    import org.apache.hadoop.fs.Path
    val input = new Path("./src/test/resources/xml")
    val output = new Path("./target/output/xml")
    val fs = FileSystem.getLocal(conf)
    fs.delete(output, true) // delete old output

    val driver = new ParseJob
    driver.setConf(conf)
    val exitCode = driver.run(Array[String](input.toString, output.toString))
    exitCode should be(0)

  }

  private def lineToResult(arg: String): Array[Double] = {
    arg.split("\\s+").map(_.toDouble)
  }

}
