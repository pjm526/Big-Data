import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class CustomRecordReader extends RecordReader<LongWritable, Text> {

    private LongWritable key = new LongWritable();
    private Text value = new Text();
    private long start;
    private long end_pos;
    private Path path;
    private LineReader line_reader;
    private long initial_pos;
    
    private FSDataInputStream fsDataInputStream;
    
    private static final byte[] OPEN_BRACE = "{".getBytes();
    private static final byte[] CLOSE_BRACE = "}".getBytes();
    private static final byte[] QUOTE_CHAR = "\"".getBytes();
    private static final byte[] ESCAPE_CHAR = "\\".getBytes();

    
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        if(split != null && context != null) {
            Configuration conf = context.getConfiguration();
            if(split instanceof FileSplit) {
                FileSplit fileSplit = (FileSplit)split;
                start = fileSplit.getStart();
                end_pos = start + fileSplit.getLength();
                if(start != 0) {
                    start--;
                }
                initial_pos = start;
                path = fileSplit.getPath();
                FileSystem fSystem = path.getFileSystem(conf);
                fsDataInputStream = fSystem.open(path);
                fsDataInputStream.seek(start);
                line_reader = new LineReader(fsDataInputStream, conf);
            }
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (fsDataInputStream.getPos() < end_pos) {
            int depth = 0;
            long objPos = fsDataInputStream.getPos();
            boolean objInQuotes = false;
            byte[] nextChar = new byte[1];

            fsDataInputStream.readFully(nextChar);

            if (!Arrays.equals(nextChar, OPEN_BRACE)) {
                throw new RuntimeException("JSON object not found at byte: " + objPos);
            }

            while (fsDataInputStream.getPos() < end_pos) {
                fsDataInputStream.readFully(nextChar);
                if (Arrays.equals(nextChar, ESCAPE_CHAR)) {
                    fsDataInputStream.seek((fsDataInputStream.getPos()+1));
                } 
                else if (Arrays.equals(nextChar, QUOTE_CHAR)) {
                    objInQuotes = !objInQuotes;
                } 
                else if (!objInQuotes) {
                    if (Arrays.equals(nextChar, OPEN_BRACE)) {
                        depth++;
                    } 
                    else if (Arrays.equals(nextChar, CLOSE_BRACE)) {
                        if (depth == 0) {
                            long objEnd = fsDataInputStream.getPos();
                            int len = (int) (objEnd - objPos);
                            byte[] readBytes = new byte[len];
                            fsDataInputStream.readFully(objPos, readBytes, 0, len);

                            value = new Text(readBytes);
                            key = new LongWritable(objPos);
                            return true;
                        } 
                        else {
                            depth--;
                        }
                    }
                }
            }
            throw new RuntimeException("JSON object not found at byte: " + objPos);
        }
        return false;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if(initial_pos == end_pos) {
            return 0;
        }
        return Math.min(1.0f, (start - initial_pos) / (float) (end_pos - initial_pos));
    }

    @Override
    public void close() throws IOException {
        if(line_reader != null) {
            line_reader.close();
        }
    }

}
