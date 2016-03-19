/*
 * Name: Mengheng Hu
 * Function: in mapper combiner 
 * Time: 02/16/2016
 * 
 */
package mxhHW4.mxhHW4;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountInMapper {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{
	  
	private Map<String, Integer> tokenMap;
	
	//initialize
    @Override
	protected void setup(Context context) throws IOException, InterruptedException {
        tokenMap = new HashMap<String, Integer>();
	}

    //mapper
	@Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while(itr.hasMoreElements()){
          String token = itr.nextToken();
          Integer count = tokenMap.get(token);
          if(count == null) count = new Integer(0);
          count+=1;
          tokenMap.put(token,count);
      }
    }
    //in-mapper combiner
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        IntWritable count = new IntWritable();
        Text word = new Text();
        Set<String> keys = tokenMap.keySet();
        for (String s : keys) {
            word.set(s);
            count.set(tokenMap.get(s));
            context.write(word,count);
        }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
	
    Configuration conf = new Configuration();
    conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
    conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
    conf.set("mapreduce.framework.name", "yarn");
    Job job = Job.getInstance(conf, "word count");
    long startTime=System.currentTimeMillis();
    job.setJarByClass(WordCountInMapper.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    long endTime=System.currentTimeMillis();
    System.out.println("Total running time: "+(endTime-startTime)+"ms");
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}