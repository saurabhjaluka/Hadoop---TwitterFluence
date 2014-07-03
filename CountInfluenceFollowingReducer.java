import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CountInfluenceFollowingReducer
  extends MapReduceBase
  implements Reducer<TextPair, Text, Text, Text>
{
  private float dampingFactor;
  
  public void configure(JobConf paramJobConf)
  {
    this.dampingFactor = paramJobConf.getFloat("DAMPING", 0.85F);
  }
  
  public void reduce(TextPair paramTextPair, Iterator<Text> paramIterator, OutputCollector<Text, Text> paramOutputCollector, Reporter paramReporter)
    throws IOException
  {
    if (!paramIterator.hasNext()) {
      return;
    }
    float f1 = this.dampingFactor * Float.parseFloat(((Text)paramIterator.next()).toString());
    float f2 = 0.0F;
    if (!paramIterator.hasNext()) {
      return;
    }
    int i = Integer.parseInt(((Text)paramIterator.next()).toString());
    if (i == 0) {
      return;
    }
    f2 = f1 / i;
    while (paramIterator.hasNext())
    {
      Text localText2 = (Text)paramIterator.next();
      Text localText1 = new Text(Float.toString(f2).getBytes());
      paramOutputCollector.collect(localText2, localText1);
    }
  }
}