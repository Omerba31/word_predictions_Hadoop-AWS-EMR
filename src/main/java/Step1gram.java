import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class Step1gram {

    /**
     * Mapper class:
     * Processes each line, filters out stop words, and emits one-grams with their occurrences.
     */

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        HashMap<String, Integer> stopWords = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            Main.generateStopWord(context, stopWords);
        }

        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // Parse the input line (n-gram \t year \t occurrences \t pages \t books)
            String[] fields = value.toString().split("\t");
            String oneGram = fields[0].trim();      // 1st field is the 1-gram
            String occurrences = fields[2].trim(); // 3rd field is the occurrences

            if (!stopWords.containsKey(oneGram)) {
                context.write(new Text(oneGram), new Text(occurrences));
            }
        }
    }

    /**
     * Reducer class:
     * Aggregates occurrences for each one-gram.
     */

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long sumOccurrences = 0L;
            for (Text value : values) {
                try {
                    sumOccurrences += Long.parseLong(value.toString());
                } catch (NumberFormatException e) {
                    // Handle invalid number format gracefully
                    context.getCounter("Step1gram", "InvalidOccurrences").increment(1);
                }
            }
            context.write(key, new Text(Long.toString(sumOccurrences)));
        }
    }

    /**
     * Partitioner class:
     * Distributes 1grams to reducers based on hash code.
     */

    public static class Partition extends Partitioner<Text, Text> {

        public int getPartition(Text key, Text value, int numPartitions) {

            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
}