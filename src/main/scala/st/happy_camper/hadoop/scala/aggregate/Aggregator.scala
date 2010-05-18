/*
 * Copyright 2010 Happy-Camper Street.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
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

/**
 * @author ueshin
 */
object Aggregator extends Configured with Tool {

  private class Map extends Mapper[LongWritable, Text, AccessWritable, IntWritable] {

    type Context = Mapper[LongWritable, Text, AccessWritable, IntWritable]#Context

    implicit def accessToAccessWritable(access: Access) = new AccessWritable(access)
    implicit def intToIntWritable(i: Int) = new IntWritable(i)

    private val PATTERN = ("^(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}) .* " +
                        "\\[(\\d{2}/[A-Z][a-z][a-z]/\\d{4}):\\d{2}:\\d{2}:\\d{2} [-+]\\d{4}\\] " +
                        "\"GET ((?:/[^ ]*)?/(?:[^/]+\\.html)?) HTTP/1\\.[01]\" (?:200|304) .*$").r

    private val one = 1

    override def map(key: LongWritable, value: Text, context: Context) {
       value.toString match {
         case PATTERN(ip, accessDate, url) => {
           try {
             context.write(new Access(ip, if(url.endsWith("/")) { url + "index.html" } else { url }, dateFormat.parse(accessDate)), one)
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

    type Context = Reducer[AccessWritable, IntWritable, AccessWritable, IntWritable]#Context

    implicit def intToIntWritable(i: Int) = new IntWritable(i)

    override def reduce(key: AccessWritable, values: java.lang.Iterable[IntWritable], context: Context) {
      context.write(key, values.foldLeft(0) { _ + _.get })
    }
  }

  private class Reduce extends Reducer[AccessWritable, IntWritable, Text, IntWritable] {

    type Context = Reducer[AccessWritable, IntWritable, Text, IntWritable]#Context

    implicit def stringToText(str: String) = new Text(str)
    implicit def intToIntWritable(i: Int) = new IntWritable(i)

    override def reduce(key: AccessWritable, values: java.lang.Iterable[IntWritable], context: Context) {
      context.write(
        "%s\t%s\t%s".format(key.access.ip, key.access.url, dateFormat.format(key.access.accessDate)),
        values.foldLeft(0) { _ + _.get }
      )
    }

    private def dateFormat = new SimpleDateFormat("yyyy/MM/dd")
  }

  def run(args: Array[String]) = {
    val job = new Job(getConf, "aggregator")

    job.setJarByClass(getClass)

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
