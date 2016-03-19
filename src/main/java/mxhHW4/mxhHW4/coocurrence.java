/*
 * name:Mengheng Hu
 * function:co-occurrence stripes design pattern
 * Time: 02/17/2016
 */
package mxhHW4.mxhHW4;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.AbstractMapWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class coocurrence extends Configured implements Tool{
	private static final Logger LOG = Logger.getLogger(coocurrence.class);
	public static class mapWritable extends MapWritable{
		
	    private Map<Writable,Writable> instance;
	    //private Writable key;
	    //private Writable value;
		/** Default constructor. */
		public mapWritable() {
			super();
			this.instance = new HashMap<Writable,Writable>();
			//this.key = instance.get(key);
			//this.value = instance.get(value);
	    }
	    @Override
		public String toString(){
	    	Iterator<Entry<Writable,Writable>> i = entrySet().iterator();
	         if (! i.hasNext())
	            return "{}";

	        StringBuilder sb = new StringBuilder();
	        sb.append('{');
	        for (;;) {
	            Entry<Writable,Writable> e = i.next();
	            Writable key = e.getKey();
	            Writable value = e.getValue();
	            sb.append(key   == this ? "(this Map)" : key);
	            sb.append('=');
	            sb.append(value == this ? "(this Map)" : value);
	            if (! i.hasNext())
	                return sb.append('}').toString();
	            sb.append(", ");
	        }
		}
	}
	
	//static String STOP_WORDS_FILE;
	/*
	 * stripes design pattern mapper
	 */
	public static class StripesOccurrenceMapper extends Mapper<LongWritable,Text,Text,mapWritable> {
		  	private mapWritable occurrenceMap =  new mapWritable();
		  	private Text word = new Text();
			private Set<String> stopWords = new HashSet<String>();
			private BufferedReader fis;
			
			private static String input;
			private static boolean caseSensitive;
			private static int WORDS_LEN;
			
			 protected void setup(Mapper.Context context)
				        throws IOException,
				        InterruptedException {
				    	Configuration conf = context.getConfiguration();
				      if (context.getInputSplit() instanceof FileSplit) {
				        this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
				      } else {
				        this.input = context.getInputSplit().toString();
				      }
				      Configuration config = context.getConfiguration();
				      this.caseSensitive = config.getBoolean("coocurrence.case.sensitive", false);
				      if (config.getBoolean("coocurrence.skip.patterns", false)) {
				        URI[] localPaths = context.getCacheFiles();
				        parseSkipFile(localPaths[0]);
				      }
				    }

				    private void parseSkipFile(URI patternsURI) {
				      LOG.info("Added file to the distributed cache: " + patternsURI);
				      
				      try {
				    	    String path1 = patternsURI.getPath();
				Path pt = new Path(path1);

				LOG.info("Added file to the distributed cache: " + pt);
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				String pattern = br.readLine();
				while (pattern != null) {
					stopWords.add(pattern);
					pattern = br.readLine();
				}
				} catch (Exception e) {
					e.printStackTrace();
				}
		}	      
		//mapper method
		  @Override
		 protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		   int neighbors = context.getConfiguration().getInt("neighbors", 2);
		   WORDS_LEN = context.getConfiguration().getInt("WORDS_LEN", 5);
		   //Configuration conf = context.getConfiguration();
		   //WORDS_LEN = 5;
		   String value2 = value.toString().replaceAll("\\pP|\\pS", " ");
		   String[] tokens = value2.split("\\s+");
		   if (tokens.length > 1) {
			   /*
			    * skip stop words and determine whether the word is not fit to the length requirement
			    */
		      for (int i = 0; i < tokens.length; i++) {
		    	  if (tokens[i].isEmpty() || stopWords.contains(tokens[i])) {
		  	            continue;
		  	        }
		    	  if(tokens[i].length()==WORDS_LEN){ 
			    	  word.set(tokens[i]);
		    	  }
		    		  occurrenceMap.clear();
		          int start = (i - neighbors < 0) ? 0 : i - neighbors;
		          int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
		           for (int j = start; j <= end; j++) {
		                if (tokens[j].equals(tokens[i])) continue;
		                if (tokens[j].isEmpty() || stopWords.contains(tokens[j])) continue;
		                if(tokens[j].length()==WORDS_LEN) {
			                Text neighbor = new Text(tokens[j]);
			                if(occurrenceMap.containsKey(neighbor)){
			                   IntWritable count = (IntWritable)occurrenceMap.get(neighbor);
			                   count.set(count.get()+1);
		                }else{
		                   occurrenceMap.put(neighbor,new IntWritable(1));
		                }
		                }
		           }
		          context.write(word,occurrenceMap);
		     }
		   }
		  }
		}
	
	/*
	 * reducer
	 */
	public static class StripesReducer extends Reducer<Text, mapWritable, Text, mapWritable> {
	    private mapWritable incrementingMap = new mapWritable();

	    @Override
	    protected void reduce(Text key, Iterable<mapWritable> values, Context context) throws IOException, InterruptedException {
	        incrementingMap.clear();
	        for (mapWritable value : values) {
	            addAll(value);
	        }
	        context.write(key, incrementingMap);
	    }
	    
	    protected void addAll(mapWritable resultmap) {
	        Set<Writable> keys = resultmap.keySet();
	        for (Writable key : keys) {
	            IntWritable fromCount = (IntWritable) resultmap.get(key);
	            if (incrementingMap.containsKey(key)) {
	                IntWritable count = (IntWritable) incrementingMap.get(key);
	                count.set(count.get() + fromCount.get());
	            } else {
	                incrementingMap.put(key, fromCount);
	            }
	        }
	    }
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new coocurrence(), args);
	    System.exit(res);
    }
 
    public int run(String[] args) throws Exception {
    	Configuration conf = new Configuration();
	    //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
	    conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
	    conf.set("mapreduce.framework.name", "yarn");
	    //conf.set("STOP_WORDS_FILE", args[2]);
	    conf.set("WORDS_LEN", args[2]);
	    Job job = Job.getInstance(conf, "co-occurence");
	    for (int i = 0; i < args.length; i += 1) {
	        if ("-skip".equals(args[i])) {
	          job.getConfiguration().setBoolean("coocurrence.skip.patterns", true);
	          i += 1;
	          job.addCacheFile(new Path(args[i]).toUri());
	          // this demonstrates logging
	          LOG.info("Added file to the distributed cache: " + args[i]);
	          
	        }
	      }
	    job.setJarByClass(coocurrence.class);
	    job.setMapperClass(StripesOccurrenceMapper.class);
	    //job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(StripesReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(mapWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    //System.exit(job.waitForCompletion(true) ? 0 : 1);
	    return job.waitForCompletion(true) ? 0 : 1;
    }
	/*
	 * main function
	 */
	
 }
