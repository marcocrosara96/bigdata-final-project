package inputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * InputFormat che propone ai mapper non una linea di testo ma più linee che corrispondono alla pagina in plain text +
 * l'intestazione della stessa contenente l'url. Sfrutta NLinesRecordReader per lo split corretto di tali pagine.
 */
public class PageAndHeaderInputFormat extends FileInputFormat<LongWritable, Text> {

    @Override
    public RecordReader<LongWritable,Text> createRecordReader(InputSplit arg0, TaskAttemptContext arg1){
        return new NLinesRecordReader();
    }
}