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

public class InvertInputMapper
  extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, Text>
{
  public void map(LongWritable paramLongWritable, Text paramText, OutputCollector<Text, Text> paramOutputCollector, Reporter paramReporter)
    throws IOException
  {
    ArrayList localArrayList = new ArrayList();
    Text localText1 = new Text();
    Text localText2 = new Text();
    String str1 = paramText.toString();
    StringTokenizer localStringTokenizer = new StringTokenizer(str1);
    while (localStringTokenizer.hasMoreTokens()) {
      localArrayList.add(localStringTokenizer.nextToken());
    }
    if (localArrayList.size() >= 2)
    {
      String str2 = (String)localArrayList.get(0);
      String str3 = (String)localArrayList.get(1);
      localText1.set(str2.getBytes());
      localText2.set(str3.getBytes());
      paramOutputCollector.collect(localText2, localText1);
    }
  }
}