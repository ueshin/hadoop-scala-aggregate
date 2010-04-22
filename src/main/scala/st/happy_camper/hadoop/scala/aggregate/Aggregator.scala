package st.happy_camper.hadoop.scala.aggregate

import _root_.scala.collection.JavaConversions._

import _root_.java.io._
import _root_.java.text._
import _root_.java.util._

import _root_.org.apache.hadoop.conf._
import _root_.org.apache.hadoop.fs._
import _root_.org.apache.hadoop.io._
import _root_.org.apache.hadoop.mapreduce._
import _root_.org.apache.hadoop.mapreduce.lib.input._
import _root_.org.apache.hadoop.mapreduce.lib.output._
import _root_.org.apache.hadoop.util._

object Aggregator extends Configured with Tool {

  private class Map extends Mapper[LongWritable, Text, AccessWritable, IntWritable] {

    private val PATTERN = ("^(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}) .* " +
                        "\\[(\\d{2}/[A-Z][a-z][a-z]/\\d{4}):\\d{2}:\\d{2}:\\d{2} [-+]\\d{4}\\] " +
                        "\"GET ((?:/[^ ]*)?/(?:[^/]+\\.html)?) HTTP/1\\.[01]\" (?:200|304) .*$").r

    private val access = new AccessWritable
    private val one = new IntWritable(1)

    override def map(
       key: LongWritable,
       value: Text,
       context: Mapper[LongWritable, Text, AccessWritable, IntWritable]#Context
     ) {
       value.toString match {
         case PATTERN(ip, accessDate, url) => {
           try {
             access.access = new Access(ip, if(url.endsWith("/")) { url + "index.html" } else { url }, dateFormat.parse(accessDate))
             context.write(access, one)
           }
           catch {
             case e: ParseException => e.printStackTrace()
           }
         }
         case _ =>
       }
    }

    private def dateFormat = new SimpleDateFormat("dd/MMM/yyyy", Locale.US)
  }

  private class Combine extends Reducer[AccessWritable, IntWritable, AccessWritable, IntWritable] {

    override def reduce(
      key: AccessWritable,
      values: java.lang.Iterable[IntWritable],
      context: Reducer[AccessWritable, IntWritable, AccessWritable, IntWritable]#Context
    ) {
      context.write(key, new IntWritable(values.foldLeft(0) { _ + _.get }))
    }
  }

  private class Reduce extends Reducer[AccessWritable, IntWritable, Text, IntWritable] {

    override def reduce(
      key: AccessWritable,
      values: java.lang.Iterable[IntWritable],
      context: Reducer[AccessWritable, IntWritable, Text, IntWritable]#Context
    ) {
      context.write(
        new Text("%s\t%s\t%s".format(key.access.ip, key.access.url, dateFormat.format(key.access.accessDate))),
        new IntWritable(values.foldLeft(0) { _ + _.get })
      )
    }

    private def dateFormat = new SimpleDateFormat("yyyy/MM/dd")
  }

  def run(args: Array[String]) = {
    val job = new Job(getConf, "aggregator")

    FileInputFormat.setInputPaths(job, args(0))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    job.setMapperClass(classOf[Map])
    job.setCombinerClass(classOf[Combine])
    job.setReducerClass(classOf[Reduce])

    job.setMapOutputKeyClass(classOf[AccessWritable])
    job.setMapOutputValueClass(classOf[IntWritable])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    job.waitForCompletion(true)

    0
  }

  def main(args: Array[String]) {
    System.exit(ToolRunner.run(this, args))
  }
}
