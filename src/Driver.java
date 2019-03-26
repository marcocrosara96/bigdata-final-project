import map.Map;

import reduce.Reduce;
import inputFormat.PageAndHeaderInputFormat;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Driver for MapReduce Job in Java.
 * The driver class configures and submits the job to the Hadoop cluster for execution.
 */
public class Driver extends Configured implements Tool {
    /*--- CONFIGURATION ----*/
    public static final String JOB_NAME = "Analyzer";
    public static final String INPUT_PATH_WET = "/input/wet/00000.wet"; //input file path or input directory path
    public static final String INPUT_PATH_INFO = "/input/info/info-00000.info"; //input file path or input directory path
    public static String OUTPUT_PATH = "/output/" + dateUniqueId(); //output path
    public static final int NUM_REDUCE_TASK = 1;
    /*----------------------*/

    /**
     * Main
     * @param args --> formato: hadoop jar DATA/analyzer.jar [input file/dir WET] [ input file/dir INFO] [output dir] [# reducers]
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Driver(), args); //(args: <file input>, <dir output>, <#reducer>)
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), JOB_NAME);
        job.setJarByClass(this.getClass());
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // ######################################################
        // ############### WET INPUT FILES/DIRS #################
        // ------ N.B. Sostituito con l'uso di MultipleInputs -------
        // FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
        // job.setInputFormatClass(PageAndHeaderInputFormat.class); //Set the new input format class
        // ----------------------------------------------------------
        if(args.length > 0)
            MultipleInputs.addInputPath(job, new Path(args[0]), PageAndHeaderInputFormat.class);
        else
            MultipleInputs.addInputPath(job, new Path(INPUT_PATH_WET), PageAndHeaderInputFormat.class);
        // ############### INFO INPUT FILES/DIRS ################
        if(args.length > 1)
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class);
        else
            MultipleInputs.addInputPath(job, new Path(INPUT_PATH_INFO), TextInputFormat.class);
        // ################# OUTPUT FILES/DIRS ##################
        if(args.length > 2)
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
        else
            FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        // ################# NUMBER OF REDUCERS #################
        if(args.length > 3)
            job.setNumReduceTasks(Integer.parseInt(args[3]));
        else
            job.setNumReduceTasks(NUM_REDUCE_TASK); //Default = 1
        // ######################################################

        //Set Distributed Cache (Dictionary Files)
        DistributedCache.addCacheFile(new Path(Map.DICTIONARY_PATH).toUri(), job.getConfiguration());

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Uso la classe Date per ritornare un id univoco per la directory di output
     * @return id univoco con formato yyyyMMdd_HHmmss
     */
    public static String dateUniqueId(){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
        Date date = new Date();
        return dateFormat.format(date);
    }
}
