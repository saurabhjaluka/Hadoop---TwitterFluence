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

public class CountInfluenceFollowingMapper
  extends MapReduceBase
  implements Mapper<LongWritable, Text, TextPair, Text>
{
  private static final String countIdentifier = "#";
  private static final String influenceIdentifier = "@";
  private static final String influenceLocation = "0";
  private static final String countLocation = "1";
  private static final String followingLocation = "2";
  
  public void map(LongWritable paramLongWritable, Text paramText, OutputCollector<TextPair, Text> paramOutputCollector, Reporter paramReporter)
    throws IOException
  {
    ArrayList localArrayList = new ArrayList();
    TextPair localTextPair = new TextPair(new Text(), new Text());
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
      localTextPair.setFirst(str2);
      String str4;
      if (str1.endsWith("@"))
      {
        localTextPair.setSecond("0");
        str4 = str3.substring(0, str3.length() - 1);
        localText.set(str4.getBytes());
      }
      else if (str1.endsWith("#"))
      {
        localTextPair.setSecond("1");
        str4 = str3.substring(0, str3.length() - 1);
        localText.set(str4.getBytes());
      }
      else
      {
        localTextPair.setSecond("2");
        str4 = str3;
        localText.set(str4.getBytes());
      }
      paramOutputCollector.collect(localTextPair, localText);
    }
  }
}