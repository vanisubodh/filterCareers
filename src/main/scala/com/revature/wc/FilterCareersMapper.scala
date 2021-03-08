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
    
        
        // We get one record from CC Index after it was filtered based on URL. Only "career" links here.
        // We need to see if the page actually contains a job listing

     // Our record schema is like this --
     // url_host_name, url, warc_filename, warc_record_offset, warc_record_length
   
    val line = value.toString() 
    val fields=line.split(",")
   
   // extract fields
    val url_host_name =fields(0)
    val url =fields(1)
    val  warc_filename=fields(2)
    val  warc_record_offset=new StringOps(fields(3)).toLong
    val  warc_record_length=new StringOps(fields(4)).toInt

    // get content from the WARC in S3.
    val content=getContent("commoncrawl", warc_filename, warc_record_offset,warc_record_length)
      // if the text contains filter words

      // See if the content contains filter expressions.

    if( content.contains("Job Description")){
    
      // We write the URL to output
        context.write(new Text(url), new IntWritable(1)) 
    }
    else
    {
   // By not writing anything in the output, we are removing this URL

    }
    
    
  }

   def getContent(bucket:String, filename:String, record_offset:Long, record_length:Int):String={
      
    // Get the key and secret from environment
      val key=sys.env("DAS_KEY_ID")
      val secret=sys.env("DAS_SEC")

      // Get Credentials for AWS
      val awsCreds = new BasicAWSCredentials(key, secret);
      //Get the s3 Client
      val s3client = AmazonS3ClientBuilder.standard()
                            .withRegion(Regions.US_EAST_1)
                              .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                              .build();

      // Read the Content of WARC record
      val s3Object = s3client.getObject(new GetObjectRequest( bucket, filename).withRange(record_offset,record_offset+record_length))
      val s3iputStream = s3Object.getObjectContent()
      var buff=new Array[Byte](record_length+1)
      val count=s3iputStream.read(buff, 0, record_length)
      val raw_text=new String(buff, StandardCharsets.US_ASCII)
      s3iputStream.close()
      raw_text
   }
}
