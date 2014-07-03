import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupByComparator
  extends WritableComparator
{
  protected GroupByComparator()
  {
    super(TextPair.class, true);
  }
  
  public int compare(WritableComparable paramWritableComparable1, WritableComparable paramWritableComparable2)
  {
    TextPair localTextPair1 = (TextPair)paramWritableComparable1;
    TextPair localTextPair2 = (TextPair)paramWritableComparable2;
    return localTextPair1.getFirst().compareTo(localTextPair2.getFirst());
  }
}