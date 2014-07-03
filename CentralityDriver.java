import java.io.IOException;
import java.io.PrintStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class CentralityDriver
{
  public static void main(String[] paramArrayOfString)
    throws IOException
  {
    if (paramArrayOfString.length != 7)
    {
      System.err.println("Usage: WordCount <in> <out> <num_users> <damping> <num_runs> <num_reducers> <print_intermediate>");
      System.exit(1);
    }
    String str1 = paramArrayOfString[0];
    String str2 = paramArrayOfString[1];
    int i = Integer.parseInt(paramArrayOfString[2]);
    float f = Float.parseFloat(paramArrayOfString[3]);
    int j = Integer.parseInt(paramArrayOfString[4]);
    int k = Integer.parseInt(paramArrayOfString[5]);
    int m = Integer.parseInt(paramArrayOfString[6]) == 1 ? 1 : 0;
    
    int n = 0;
    


    JobConf localJobConf = new JobConf(CentralityDriver.class);
    localJobConf.setJobName("InvertInput");
    localJobConf.setOutputKeyClass(Text.class);
    localJobConf.setOutputValueClass(Text.class);
    localJobConf.setMapperClass(InvertInputMapper.class);
    localJobConf.setReducerClass(InvertInputReducer.class);
    localJobConf.setInputFormat(TextInputFormat.class);
    localJobConf.setOutputFormat(TextOutputFormat.class);
    FileInputFormat.setInputPaths(localJobConf, new Path[] { new Path(str1) });
    FileOutputFormat.setOutputPath(localJobConf, new Path(str1 + "/inverted"));
    localJobConf.setNumReduceTasks(k);
    localJobConf.setJarByClass(CentralityDriver.class);
    JobClient.runJob(localJobConf);
    

    localJobConf.setJobName("FollowerCount");
    localJobConf.setOutputKeyClass(Text.class);
    localJobConf.setOutputValueClass(Text.class);
    localJobConf.setMapperClass(FollowingCountMapper.class);
    localJobConf.setReducerClass(FollowingCountReducer.class);
    localJobConf.setInputFormat(TextInputFormat.class);
    localJobConf.setOutputFormat(TextOutputFormat.class);
    FileInputFormat.setInputPaths(localJobConf, new Path[] { new Path(str1 + "/inverted") });
    FileOutputFormat.setOutputPath(localJobConf, new Path(str2 + "/count"));
    localJobConf.setNumReduceTasks(k);
    localJobConf.setJarByClass(CentralityDriver.class);
    JobClient.runJob(localJobConf);
    
    localJobConf = new JobConf(CentralityDriver.class);
    localJobConf.setJobName("InitializeInfluence");
    localJobConf.setOutputKeyClass(Text.class);
    localJobConf.setOutputValueClass(Text.class);
    localJobConf.setMapperClass(InitializeInfluenceMapper.class);
    localJobConf.setReducerClass(InitializeInfluenceReducer.class);
    localJobConf.setInputFormat(TextInputFormat.class);
    localJobConf.setOutputFormat(TextOutputFormat.class);
    FileInputFormat.setInputPaths(localJobConf, new Path[] { new Path(str1 + "/inverted") });
    FileOutputFormat.setOutputPath(localJobConf, new Path(str2 + "/influence" + n));
    localJobConf.setNumReduceTasks(k);
    localJobConf.setInt("USERS", i);
    localJobConf.setJarByClass(CentralityDriver.class);
    JobClient.runJob(localJobConf);
    
    FileSystem localFileSystem = FileSystem.get(localJobConf);
    while (n < 2 * j)
    {
      localJobConf = new JobConf(CentralityDriver.class);
      localJobConf.setJobName("CountInfluenceFollowingJoin");
      
      localJobConf.setOutputKeyClass(Text.class);
      localJobConf.setOutputValueClass(Text.class);
      
      localJobConf.setMapperClass(CountInfluenceFollowingMapper.class);
      
      localJobConf.setReducerClass(CountInfluenceFollowingReducer.class);
      localJobConf.setMapOutputKeyClass(TextPair.class);
      
      localJobConf.setInputFormat(TextInputFormat.class);
      localJobConf.setOutputFormat(TextOutputFormat.class);
      
      localJobConf.setPartitionerClass(TextPairFirstPartitioner.class);
      localJobConf.setOutputValueGroupingComparator(GroupByComparator.class);
      

      FileInputFormat.setInputPaths(localJobConf, new Path[] { new Path(str1 + "/inverted") });
      
      Path localPath = new Path(str2 + "/influence" + n);
      FileInputFormat.addInputPath(localJobConf, localPath);
      
      FileInputFormat.addInputPath(localJobConf, new Path(str2 + "/count"));
      FileOutputFormat.setOutputPath(localJobConf, new Path(str2 + "/influence" + (n + 1)));
      localJobConf.setNumReduceTasks(k);
      localJobConf.setFloat("DAMPING", f);
      localJobConf.setJarByClass(CentralityDriver.class);
      JobClient.runJob(localJobConf);
      if ((n != 0) && (m == 0) && (localFileSystem.exists(localPath))) {
        localFileSystem.delete(localPath, true);
      }
      n++;
      
      localJobConf = new JobConf(CentralityDriver.class);
      localJobConf.setJobName("AggregateInfluence");
      
      localJobConf.setOutputKeyClass(Text.class);
      localJobConf.setOutputValueClass(Text.class);
      localJobConf.setMapOutputKeyClass(Text.class);
      
      localJobConf.setMapperClass(AggregateInfluenceMapper.class);
      localJobConf.setReducerClass(AggregateInfluenceReducer.class);
      
      localJobConf.setInputFormat(TextInputFormat.class);
      localJobConf.setOutputFormat(TextOutputFormat.class);
      

      FileInputFormat.setInputPaths(localJobConf, str2 + "/influence" + n);
      FileOutputFormat.setOutputPath(localJobConf, new Path(str2 + "/influence" + (n + 1)));
      localJobConf.setNumReduceTasks(k);
      localJobConf.setInt("USERS", i);
      localJobConf.setFloat("DAMPING", f);
      localJobConf.setJarByClass(CentralityDriver.class);
      JobClient.runJob(localJobConf);
      

      localPath = new Path(str2 + "/influence" + n);
      if ((m == 0) && (localFileSystem.exists(localPath))) {
        localFileSystem.delete(localPath, true);
      }
      n++;
    }
    localJobConf = new JobConf(CentralityDriver.class);
    localJobConf.setJobName("InvertOutput");
    localJobConf.setOutputKeyClass(FloatWritable.class);
    localJobConf.setOutputValueClass(Text.class);
    localJobConf.setMapperClass(InvertOutputMapper.class);
    localJobConf.setReducerClass(InvertOutputReducer.class);
    localJobConf.setInputFormat(TextInputFormat.class);
    localJobConf.setOutputFormat(TextOutputFormat.class);
    FileInputFormat.setInputPaths(localJobConf, new Path[] { new Path(str2 + "/influence" + n) });
    FileOutputFormat.setOutputPath(localJobConf, new Path(str2 + "/influencefinal"));
    localJobConf.setNumReduceTasks(1);
    localJobConf.setJarByClass(CentralityDriver.class);
    JobClient.runJob(localJobConf);
    
    System.exit(0);
  }
}
