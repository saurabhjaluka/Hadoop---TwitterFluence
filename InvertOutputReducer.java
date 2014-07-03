import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class InvertOutputReducer
  extends MapReduceBase
  implements Reducer<FloatWritable, Text, FloatWritable, Text>
{
  public void reduce(FloatWritable paramFloatWritable, Iterator<Text> paramIterator, OutputCollector<FloatWritable, Text> paramOutputCollector, Reporter paramReporter)
    throws IOException
  {
    while (paramIterator.hasNext()) {
      paramOutputCollector.collect(paramFloatWritable, paramIterator.next());
    }
  }
}
