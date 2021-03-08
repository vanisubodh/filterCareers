package com.revature.FilterCareers

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable


/**
  *
  */
object FilterCareersDriver {
  def main(args : Array[String]): Unit = {
    if(args.length != 2) {
      println("Usage: FilterCareers <input dir> <output dir>")
      System.exit(-1)
    }

    //we start by instantiating a Job object we can configure
    val job = Job.getInstance()

    //we set the jar file that countains our job -- we specify this class
    job.setJarByClass(FilterCareersDriver.getClass())

    job.setJobName("FilterCareers")

    //we set the input format, we're (almost) always going to be dealing with TextInputFormat
    job.setInputFormatClass(classOf[TextInputFormat])

    //set the file input and output based on the args passed in
    FileInputFormat.setInputPaths(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    //The above is mostly boilerplate, you'll want something very similar in all your MR jobs
    // below here we configure specific mappers and reducers, and we set the output key-value pair
    // types based on the specifics -- what our job does.

    //Specify mapper and reducer:
    job.setMapperClass(classOf[FilterCareersMapper])
    job.setReducerClass(classOf[FilterCareersReducer])

    //specify output types, we're making use of some defaults to not specify more
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    val success = job.waitForCompletion(true) //submit configured job + wait for it to be done
    System.exit(if (success) 0 else 1) //exit with 0 if successful

  }
}