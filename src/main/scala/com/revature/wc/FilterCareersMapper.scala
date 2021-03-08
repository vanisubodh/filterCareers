package com.revature.FilterCareers

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model._;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.auth._
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;   
import java.nio.charset.StandardCharsets
import scala.collection.immutable.StringOps
import java.io.FileReader
/**  */
class FilterCareersMapper extends Mapper[LongWritable, Text, Text, IntWritable] {

  /**
    * Defines our map transformation, turning an input k-v pair into 0 or more intermediate k-v pairs
    * 
    * we need to manually specify the typing for Context here
    *
    * @param key
    * @param value
    * @param context
    */
  override def map(
      key: LongWritable,
      value: Text,
      context: Mapper[LongWritable, Text, Text, IntWritable]#Context
      ): Unit = {
    //we actually write our logic in here.  We create output by using context.write
    val line = value.toString() // url_host_name, url, warc_filename, warc_record_offset, warc_record_length
    val fields=line.split(",")
    val url_host_name =fields(0)
    val url =fields(1)
    val  warc_filename=fields(2)
    val  warc_record_offset=new StringOps(fields(3)).toLong
    val  warc_record_length=new StringOps(fields(4)).toInt

    val content=getContent("commoncrawl", warc_filename, warc_record_offset,warc_record_length)
      // if the text contains filter words
    if( content.contains("Job Description"))
        context.write(new Text(url), new IntWritable(1))
    
    //0 hi from scala hadoop =>
    //hi 1
    //from 1
    //scala 1
    //hadoop 1
  }

   def getContent(bucket:String, filename:String, record_offset:Long, record_length:Int):String={
      val key=sys.env("DAS_KEY_ID")
      val secret=sys.env("DAS_SEC")
      val awsCreds = new BasicAWSCredentials(key, secret);
      val s3 = AmazonS3ClientBuilder.standard()
                            .withRegion(Regions.US_EAST_1)
                              .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                              .build();

      val o = s3.getObject(new GetObjectRequest( bucket, filename).withRange(record_offset,record_offset+record_length))
      val s3is = o.getObjectContent()
      var buff=new Array[Byte](record_length+1)
      val n=s3is.read(buff, 0, record_length)
      val raw_text=new String(buff, StandardCharsets.US_ASCII)
      raw_text
   }
}
