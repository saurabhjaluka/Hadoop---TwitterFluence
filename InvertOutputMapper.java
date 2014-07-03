import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class InvertOutputMapper
  extends MapReduceBase
  implements Mapper<LongWritable, Text, FloatWritable, Text>
{
  public void map(LongWritable paramLongWritable, Text paramText, OutputCollector<FloatWritable, Text> paramOutputCollector, Reporter paramReporter)
    throws IOException
  {
    ArrayList localArrayList = new ArrayList();
    FloatWritable localFloatWritable = new FloatWritable();
    Text localText = new Text();
    String str1 = paramText.toString();
    StringTokenizer localStringTokenizer = new StringTokenizer(str1);
    while (localStringTokenizer.hasMoreTokens()) {
      localArrayList.add(localStringTokenizer.nextToken());
    }
    if (localArrayList.size() >= 2)
    {
      String str2 = (String)localArrayList.get(0);
      String str3 = (String)localArrayList.get(1);
      localFloatWritable.set(Float.parseFloat(str3.substring(0, str3.length() - 1)));
      localText.set(str2.getBytes());
      paramOutputCollector.collect(localFloatWritable, localText);
    }
  }
}