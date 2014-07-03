import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class InitializeInfluenceReducer
  extends MapReduceBase
  implements Reducer<Text, Text, Text, Text>
{
  private static final String influenceIdentifier = "@";
  private Text initialValue;
  private int numUsers;
  
  public void configure(JobConf paramJobConf)
  {
    this.numUsers = paramJobConf.getInt("USERS", 194830);
    this.initialValue = new Text(Double.toString(1.0D / this.numUsers) + "@");
  }
  
  public void reduce(Text paramText, Iterator<Text> paramIterator, OutputCollector<Text, Text> paramOutputCollector, Reporter paramReporter)
    throws IOException
  {
    paramOutputCollector.collect(paramText, this.initialValue);
  }
}
