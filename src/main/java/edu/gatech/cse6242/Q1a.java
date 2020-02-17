package edu.gatech.cse6242;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q1a {
	public static class Q1aMapper extends Mapper<Object, Text, Text, IntWritable>{
	  // set up mapper
	  private Text src_id = new Text();
	  private IntWritable weights = new IntWritable(1);

	@Override
  /* TO DO: Needs to be implemented in this job */
  //sepcify the input key, input value, output key and output values ot the
  //map function
	  public void map(Object key, Text value, Context context)
        	  throws IOException, InterruptedException {
              //String[] split = value.toString().split("\t");
		      StringTokenizer itra = new StringTokenizer(value.toString());
		     while (itra.hasMoreTokens()) {
                 src_id.set(itra.nextToken());
                 itra.nextToken();
				 weights.set(Integer.parseInt(itra.nextToken()));
                 context.write(src_id, weights);}
	       }
	}


    public static class Q1aReducer extends Reducer
    <Text,IntWritable,Text,IntWritable> {
 	  // set up reducer
      private IntWritable result = new IntWritable();

  	@Override
    //sepcify the input key, input value, output key and output values ot the
    //reduce function
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
        int maxVal = 0;
        for (IntWritable val : values) {
	         if(maxVal < val.get())
					 {
						 maxVal = val.get();
					 }
        }
       result.set(maxVal);
       context.write(key, result);
      }
    }

//run the MapReduce job
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "Q1a");

  // locate the relvant JAR file by looking for the JAR file containing
  // this class
    job.setJarByClass(Q1a.class);

	// specify the map and reduce types
    job.setMapperClass(Q1aMapper.class);
  //  job.setCombinerClass(Q1aReducer.class);
    job.setReducerClass(Q1aReducer.class);
	// control the output types fot the map and reduce function
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
  //specify the input and output path
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    //complete Job
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
