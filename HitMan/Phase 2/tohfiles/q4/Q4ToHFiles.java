/**
 * Q4ETLReducer
 * */
package hitman.tohfiles.q4;

 import java.io.IOException;
 import org.apache.hadoop.conf.Configuration;  
 import org.apache.hadoop.fs.Path;  
 import org.apache.hadoop.hbase.HBaseConfiguration;  
 import org.apache.hadoop.hbase.client.HTable;  
 import org.apache.hadoop.hbase.client.Put;  
 import org.apache.hadoop.hbase.io.ImmutableBytesWritable;  
 import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;  
 import org.apache.hadoop.hbase.util.Bytes;  
 import org.apache.hadoop.io.LongWritable;  
 import org.apache.hadoop.io.Text;  
 import org.apache.hadoop.mapreduce.Job;  
 import org.apache.hadoop.mapreduce.Mapper;  
 import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
 import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;  
 import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
 
 public class Q4ToHFiles {
	 public static final byte[] CF = Bytes.toBytes("r");
	 public static final int EXPECTED_LENGTH = 2;
      public static class BulkLoadMap extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {       
           public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	   String line = new String(value.getBytes());
        	   String[] splits = line.split(" shit ");
        	   // skip the messed up lines
        	   line = splits[0];
        	   // get the key and value
               splits = line.split("\t");
               
               // these three values are going to be returned
               String dateLocation = splits[0];
               int rank = Integer.parseInt(splits[1]);
               String hashtagTweetids = splits[2];
               
               ImmutableBytesWritable HKey = 
                  		new ImmutableBytesWritable(Bytes.toBytes(dateLocation));  
               Put HPut = new Put(Bytes.toBytes(dateLocation));
               HPut.add(CF, Bytes.toBytes(rank), Bytes.toBytes(hashtagTweetids));
               context.write(HKey,HPut);
           }   
      }  
      
      public static void main(String[] args) throws Exception {  
           Configuration conf = HBaseConfiguration.create();  
           String inputPath = args[0];
           String outputPath = args[1];
           HTable hTable = new HTable(conf, args[2]);  
           Job job = new Job(conf,"HBase_Bulk_loader");        
           job.setMapOutputKeyClass(ImmutableBytesWritable.class);  
           job.setMapOutputValueClass(Put.class);  
           job.setSpeculativeExecution(false);  
           job.setReduceSpeculativeExecution(false);  
           job.setInputFormatClass(TextInputFormat.class);  
           job.setOutputFormatClass(HFileOutputFormat.class);  
           job.setJarByClass(Q4ToHFiles.class);  
           job.setMapperClass(Q4ToHFiles.BulkLoadMap.class);  
           FileInputFormat.setInputPaths(job, inputPath);  
           FileOutputFormat.setOutputPath(job,new Path(outputPath));             
           HFileOutputFormat.configureIncrementalLoad(job, hTable);  
           System.exit(job.waitForCompletion(true) ? 0 : 1);  
      }  
 }  