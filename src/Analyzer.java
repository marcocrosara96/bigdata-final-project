import java.io.IOException;
import java.util.regex.Pattern;

import inputFormat.PageAndHeaderInputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.log4j.Logger;

//hadoop jar analyzer.jar /input/00000_sample.wet /output/001 1
public class Analyzer extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(Analyzer.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Analyzer(), args); //Viene avviata un'istanza di World Count (args: <file input>, <dir output>, <#reducer>)
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "wordcount");
        job.setJarByClass(this.getClass());
        // Use TextInputFormat, the default unless job.setInputFormatClass is used
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(Integer.parseInt(args[2])); //se non lo mettiamo di default c'è 1 reducer
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //change input format
        job.setInputFormatClass(PageAndHeaderInputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private long numRecords = 0;
        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

        /**
         *
         * @param offset --> non la utilizzeremo mai
         * @param lineText --> verrà trasformata un una line di tipo stringa --> per essere maneggiata dai metodi di String
         * @param context --> è il canale che mi porta al reducer
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
            /*String line = lineText.toString();
            Text currentWord = new Text(); //facciamo così per evitare di ricrearlo ad ogni ciclo
            for (String word : WORD_BOUNDARY.split(line)) {
                if (word.isEmpty()) {
                    continue;
                }
                currentWord.set(word); //NB <--- Set
                context.write(currentWord, one); //one --> IntWritable (vedi sopra)
            }*/

            //System.out.println("map-line: ");
            context.write(lineText, new IntWritable(1)); //one --> IntWritable (vedi sopra)
        }
    }

    /**
     *                                        <Coppia Input>      <Coppia Output>
     *
     */
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text word, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
            /*int sum = 0;
            for (IntWritable count : counts) {
                sum += count.get();
            }
            context.write(word, new IntWritable(sum));*/
            //System.out.println("reduce-line: " + word.toString());
            context.write(word, new IntWritable(1));
        }
    }
}
