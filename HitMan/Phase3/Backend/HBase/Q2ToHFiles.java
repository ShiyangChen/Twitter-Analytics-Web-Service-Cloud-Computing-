/****
 * q2 csv to hfiles
 * */
package hitman.tohfiles.q2;

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
 
 public class Q2ToHFiles {
	 public static final String TABLE = "tweets";
	 public static final byte[] CF = Bytes.toBytes("tweetidScoreText");
      public static class BulkLoadMap extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {       
           public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	   String line = new String(value.getBytes());
        	   // abandon other appends
               String[] lines = line.split(" shit ");
               //separate the key and value
               String[] splits = lines[0].split(" ass ");
                
               String tweettimeUserid = splits[0];
                
               String tweetidScoreText = splits[1];
               tweetidScoreText = tweetidScoreText.replace(" fuck ", "\n");
               ImmutableBytesWritable HKey = 
               		new ImmutableBytesWritable(Bytes.toBytes(tweettimeUserid));  
               Put HPut = new Put(Bytes.toBytes(tweettimeUserid));
               HPut.add(CF, null, Bytes.toBytes(tweetidScoreText));
               context.write(HKey,HPut);
           }   
      }  
      
      public static void main(String[] args) throws Exception {  
           Configuration conf = HBaseConfiguration.create();  
           String inputPath = args[0];
           String outputPath = args[1];
           HTable hTable = new HTable(conf, TABLE);  
           Job job = new Job(conf,"HBase_Bulk_loader");        
           job.setMapOutputKeyClass(ImmutableBytesWritable.class);  
           job.setMapOutputValueClass(Put.class);  
           job.setSpeculativeExecution(false);  
           job.setReduceSpeculativeExecution(false);  
           job.setInputFormatClass(TextInputFormat.class);  
           job.setOutputFormatClass(HFileOutputFormat.class);  
           job.setJarByClass(Q2ToHFiles.class);  
           job.setMapperClass(Q2ToHFiles.BulkLoadMap.class);  
           FileInputFormat.setInputPaths(job, inputPath);  
           FileOutputFormat.setOutputPath(job,new Path(outputPath));             
           HFileOutputFormat.configureIncrementalLoad(job, hTable);  
           System.exit(job.waitForCompletion(true) ? 0 : 1);  
      }  
 }  