import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class FollowingCountReducer
  extends MapReduceBase
  implements Reducer<Text, Text, Text, Text>
{
  private static final String countIdentifier = "#";
  
  public void reduce(Text paramText, Iterator<Text> paramIterator, OutputCollector<Text, Text> paramOutputCollector, Reporter paramReporter)
    throws IOException
  {
    int i = 0;
    Text localText = new Text();
    while (paramIterator.hasNext()) {
      i += Integer.parseInt(((Text)paramIterator.next()).toString());
    }
    localText.set((Integer.toString(i) + "#").getBytes());
    paramOutputCollector.collect(paramText, localText);
  }
}
