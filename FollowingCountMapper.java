import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class FollowingCountMapper
  extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, Text>
{
  private static final Text one = new Text("1");
  
  public void map(LongWritable paramLongWritable, Text paramText, OutputCollector<Text, Text> paramOutputCollector, Reporter paramReporter)
    throws IOException
  {
    ArrayList localArrayList = new ArrayList();
    Text localText = new Text();
    String str1 = paramText.toString();
    StringTokenizer localStringTokenizer = new StringTokenizer(str1);
    while (localStringTokenizer.hasMoreTokens()) {
      localArrayList.add(localStringTokenizer.nextToken());
    }
    if (localArrayList.size() >= 2)
    {
      String str2 = (String)localArrayList.get(0);
      localText.set(str2.getBytes());
      paramOutputCollector.collect(localText, one);
    }
  }
}
