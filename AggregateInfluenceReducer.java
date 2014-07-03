import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AggregateInfluenceReducer
  extends MapReduceBase
  implements Reducer<Text, Text, Text, Text>
{
  private float randomness;
  private int numUsers;
  private float dampingFactor;
  private static final String influenceIdentifier = "@";
  
  public void configure(JobConf paramJobConf)
  {
    this.numUsers = paramJobConf.getInt("USERS", 194830);
    this.dampingFactor = paramJobConf.getFloat("DAMPING", 0.85F);
    this.randomness = ((1.0F - this.dampingFactor) / this.numUsers);
  }
  
  public void reduce(Text paramText, Iterator<Text> paramIterator, OutputCollector<Text, Text> paramOutputCollector, Reporter paramReporter)
    throws IOException
  {
    Text localText = new Text();
    float f = 0.0F;
    while (paramIterator.hasNext()) {
      f += Float.valueOf(((Text)paramIterator.next()).toString()).floatValue();
    }
    f = this.randomness + this.dampingFactor * f;
    localText.set((Float.toString(f) + "@").getBytes());
    paramOutputCollector.collect(paramText, localText);
  }
}
