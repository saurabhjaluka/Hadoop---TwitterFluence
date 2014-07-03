import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPair
  implements WritableComparable<Object>
{
  Text first;
  Text second;
  
  public TextPair() {}
  
  public TextPair(Text paramText1, Text paramText2)
  {
    this.first = paramText1;
    this.second = paramText2;
  }
  
  public void setFirst(String paramString)
  {
    this.first.set(paramString.getBytes());
  }
  
  public void setSecond(String paramString)
  {
    this.second.set(paramString.getBytes());
  }
  
  public Text getFirst()
  {
    return this.first;
  }
  
  public Text getSecond()
  {
    return this.second;
  }
  
  public void write(DataOutput paramDataOutput)
    throws IOException
  {
    this.first.write(paramDataOutput);
    this.second.write(paramDataOutput);
  }
  
  public void readFields(DataInput paramDataInput)
    throws IOException
  {
    if (this.first == null) {
      this.first = new Text();
    }
    if (this.second == null) {
      this.second = new Text();
    }
    this.first.readFields(paramDataInput);
    this.second.readFields(paramDataInput);
  }
  
  public int compareTo(Object paramObject)
  {
    TextPair localTextPair = (TextPair)paramObject;
    int i = getFirst().compareTo(localTextPair.getFirst());
    if (i != 0) {
      return i;
    }
    return getSecond().compareTo(localTextPair.getSecond());
  }
  
  public int hashCode()
  {
    return this.first.hashCode();
  }
  
  public boolean equals(Object paramObject)
  {
    TextPair localTextPair = (TextPair)paramObject;
    return this.first.equals(localTextPair.getFirst());
  }
}
