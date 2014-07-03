import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class TextPairFirstPartitioner
  implements Partitioner<TextPair, Writable>
{
  public void configure(JobConf paramJobConf) {}
  
  public int getPartition(TextPair paramTextPair, Writable paramWritable, int paramInt)
  {
    return Math.abs(paramTextPair.getFirst().hashCode()) % paramInt;
  }
}
