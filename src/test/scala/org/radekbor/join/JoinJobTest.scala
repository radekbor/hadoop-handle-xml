package org.radekbor.join

import org.apache.hadoop.conf.Configuration
import org.scalatest.Matchers

class JoinJobTest extends org.scalatest.FunSuite with Matchers {

  test("Should return names") {
    import org.apache.hadoop.fs.FileSystem
    val conf = new Configuration()
    conf.set("fs.default.name", "file:///")
    conf.set("mapred.job.tracker", "local")
    import org.apache.hadoop.fs.Path
    val input1 = new Path("./src/test/resources/join/city")
    val input2 = new Path("./src/test/resources/join/Peoples")
    val output = new Path("./target/output/join")
    val fs = FileSystem.getLocal(conf)
    fs.delete(output, true) // delete old output

    val driver = new JoinJob
    driver.setConf(conf)
    val exitCode = driver.run(Array[String](input1.toString, input2.toString, output.toString))
    exitCode should be(0)

  }

  private def lineToResult(arg: String): Array[Double] = {
    arg.split("\\s+").map(_.toDouble)
  }

}
