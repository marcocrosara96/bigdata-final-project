package inputFormat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LineRecordReader.LineReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class NLinesRecordReader extends RecordReader<LongWritable, Text>{
    private LineReader in;

    private LongWritable key;
    private Text value = new Text();

    private long start = 0;
    private long end = 0;
    private long pos = 0;
    private int maxLineLength;

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException,InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        }
        else {
            return Math.min(1.0f, (pos - start) / (float)(end - start));
        }
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context)throws IOException, InterruptedException {
        FileSplit split = (FileSplit) genericSplit;

        final Path file = split.getPath();
        Configuration conf = context.getConfiguration();
        this.maxLineLength = conf.getInt("mapred.linerecordreader.maxlength",Integer.MAX_VALUE);
        FileSystem fs = file.getFileSystem(conf);
        start = split.getStart();
        end= start + split.getLength();
        boolean skipFirstLine = false;
        FSDataInputStream filein = fs.open(split.getPath());

        if (start != 0){
            skipFirstLine = true;
            --start;
            filein.seek(start);
        }
        in = new LineReader(filein,conf);
        if(skipFirstLine){
            start += in.readLine(new Text(),0,(int)Math.min((long)Integer.MAX_VALUE, end - start));
        }
        this.pos = start;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null) {
            key = new LongWritable();
        }
        key.set(pos);
        if (value == null) {
            value = new Text();
        }
        value.clear();

        final Text endline = new Text("\n");
        int newSize = 0;

        Text v = new Text(); //line read on the file
        Text pageEndLine = new Text("WARC/1.0"); //delimits the end of the page and the beginning of another
        Text emptyLine = new Text("");

        //A input for the mapper is a page from "WARC/1.0" to the next "WARC/1.0" or to the end of file
        while(!(v.equals(pageEndLine)) && pos != end){ /*pos != end <-- check it isn't the end of file*/
            v = new Text();
            boolean readFullLineFlag = false;
            while (pos < end && readFullLineFlag == false) { /*se il buffer non è sufficente per leggere una riga troppo
                                                                lunga, al ciclo successivo finisco di leggerla*/
                newSize = in.readLine(  v,
                                        maxLineLength,
                                        Math.max((int)Math.min(Integer.MAX_VALUE, end-pos), maxLineLength));

                if(!v.equals(pageEndLine) && !v.equals(emptyLine)) {
                    value.append(v.getBytes(), 0, v.getLength()); //concateno la riga letta nel file all'input
                    value.append(endline.getBytes(), 0, endline.getLength()); //aggiungo newline all'input
                }

                pos += newSize;
                if (/*newSize == 0 ||*/ newSize < maxLineLength)
                    readFullLineFlag = true;
            }
        }

        if (newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
    }
}