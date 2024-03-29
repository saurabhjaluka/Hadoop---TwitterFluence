import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class InvertInputReducer
  extends MapReduceBase
  implements Reducer<Text, Text, Text, Text>
{
  public void reduce(Text paramText, Iterator<Text> paramIterator, OutputCollector<Text, Text> paramOutputCollector, Reporter paramReporter)
    throws IOException
  {
    while (paramIterator.hasNext()) {
      paramOutputCollector.collect(paramText, paramIterator.next());
    }
  }
}