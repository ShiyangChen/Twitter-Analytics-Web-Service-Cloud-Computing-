package hitman.csv2hfiles;

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
 
 public class CSV2HFiles {
	 public static final String TABLE = "tweets";
	 public static final byte[] CF = Bytes.toBytes("score_and_text");
	 public static final int LENGTH = 5;
      public static class BulkLoadMap extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {       
           public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	   String line = new String(value.getBytes());
               String[] strings = line.split("shit");
                String[] splits = strings[0].split(",");
                
                if(splits.length != LENGTH) return;
                
                String tweettime_and_userid = splits[1]+splits[0];
                
                // these three values are going to be returned
                String tweet_id = splits[2];
                String score_and_text = splits[3]+":"+splits[4];
                score_and_text = score_and_text.replace(" ass ", "\n");
                score_and_text = score_and_text.replace(" fuck ", ",");
                score_and_text = score_and_text.substring(0, score_and_text.length()-1);
                ImmutableBytesWritable HKey = 
                		new ImmutableBytesWritable(Bytes.toBytes(tweettime_and_userid));  
                Put HPut = new Put(Bytes.toBytes(tweettime_and_userid));
                HPut.add(CF, Bytes.toBytes(tweet_id), Bytes.toBytes(score_and_text));
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
           job.setJarByClass(CSV2HFiles.class);  
           job.setMapperClass(CSV2HFiles.BulkLoadMap.class);  
           FileInputFormat.setInputPaths(job, inputPath);  
           FileOutputFormat.setOutputPath(job,new Path(outputPath));             
           HFileOutputFormat.configureIncrementalLoad(job, hTable);  
           System.exit(job.waitForCompletion(true) ? 0 : 1);  
      }  
 }  