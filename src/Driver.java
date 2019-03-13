import map.Map;
import org.apache.hadoop.filecache.DistributedCache;
import reduce.Reduce;
import inputFormat.PageAndHeaderInputFormat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Driver extends Configured implements Tool {
    /*--- CONFIGURATION ----*/
    public static String JOB_NAME = "Analyzer";
    public static String INPUT_PATH = "/input/00000_sample.wet"; //input file path or input directory path
    public static String DICTIONARY_100 = "/input/dictionary_100.json";
    public static String OUTPUT_PATH = "/output/" + uniqueId(); //output path
    public static int NUM_REDUCE_TASK = 1;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Driver(), args); //Viene avviata un'istanza di World Count (args: <file input>, <dir output>, <#reducer>)
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), JOB_NAME);
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        job.setNumReduceTasks(NUM_REDUCE_TASK); //Default = 1
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Use TextInputFormat, the default unless job.setInputFormatClass is used
        job.setInputFormatClass(PageAndHeaderInputFormat.class); //Set the new input format class

        //Set Distributed Cache (Dictionary Files)
        DistributedCache.addCacheFile(new Path(DICTIONARY_100).toUri(), job.getConfiguration());

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Uso la classe Date per ritornare un id univoco per la directory di output
     * @return id univoco con formato yyyyMMdd_HHmmss
     */
    public static String uniqueId(){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
        Date date = new Date();
        return dateFormat.format(date);
    }
}
