package reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *                                <Coppia Input><Coppia Output>
 *
 */
public class Reduce extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text url, Iterable<Text> description, Context context) throws IOException, InterruptedException {
            /*int sum = 0;
            for (IntWritable count : counts) {
                sum += count.get();
            }
            context.write(word, new IntWritable(sum));*/
        //System.out.println("reduce-line: " + word.toString());
        context.write(url, description.iterator().next());
    }
}