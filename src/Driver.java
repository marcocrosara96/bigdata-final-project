import map.Map;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import reduce.Reduce;
import inputFormat.PageAndHeaderInputFormat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Driver extends Configured implements Tool {
    /*--- CONFIGURATION ----*/
    public static String JOB_NAME = "Analyzer";
    public static String INPUT_PATH_WET = "/input/wet/00000_sample.wet"; //input file path or input directory path
    public static String INPUT_PATH_INFO = "/input/info/info-00000.info"; //input file path or input directory path
    public static String OUTPUT_PATH = "/output/" + uniqueId(); //output path
    public static int NUM_REDUCE_TASK = 1;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Driver(), args); //Viene avviata un'istanza di World Count (args: <file input>, <dir output>, <#reducer>)
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), JOB_NAME);
        job.setJarByClass(this.getClass());
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //Se vengono dati in input gli argomenti, sovrascrivo i settaggi di default
        /*if(args.length > 0)
            FileInputFormat.setInputPaths(job, new Path(args[0]));
        if(args.length > 1)
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
        if(args.length > 2)
            job.setNumReduceTasks(Integer.parseInt(args[2]));*/

        job.setNumReduceTasks(NUM_REDUCE_TASK); //Default = 1
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        // ------ N.B. Sostituito con l'uso di MultipleInputs -------
        // FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
        // job.setInputFormatClass(PageAndHeaderInputFormat.class); //Set the new input format class
        // ----------------------------------------------------------
        MultipleInputs.addInputPath(job, new Path(INPUT_PATH_WET), PageAndHeaderInputFormat.class);
        MultipleInputs.addInputPath(job, new Path(INPUT_PATH_INFO), TextInputFormat.class);

        //Set Distributed Cache (Dictionary Files)
        DistributedCache.addCacheFile(new Path(Map.DICTIONARY_PATH).toUri(), job.getConfiguration());

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
