import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class Step2gram {

    /**
     * Mapper class:
     * Processes each line, filters out stop words, and emits bi-grams with their occurrences.
     */
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private final HashMap<String, Integer> stopWords = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            // Load stop words from the local file
            Main.generateStopWord(context, stopWords);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            String twoGram = fields[0].trim() + " " + fields[1].trim(); // First 2 fields are the 2-gram
            String occurrences = fields[3].trim(); // 4th field is the occurrences

            // Check if both words in the two-gram are not stop words
            String[] words = twoGram.split(" ");
            if (words.length == 2 && !stopWords.containsKey(words[0]) && !stopWords.containsKey(words[1])) {
                context.write(new Text(twoGram), new Text(occurrences));
            }
        }
    }

    /**
     * Reducer class:
     * Aggregates occurrences for each 2gram.
     */

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long sumOccurrences = 0L;
            for (Text value : values) {
                try {
                    sumOccurrences += Long.parseLong(value.toString());
                } catch (NumberFormatException e) {
                    context.getCounter("Step2gram", "InvalidOccurrences").increment(1);
                }
            }
            context.write(key, new Text(Long.toString(sumOccurrences)));
        }
    }

    /**
     * Partitioner class:
     * Distributes 2grams to reducers based on hash code.
     */
    public static class Partition extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            if (numPartitions == 0) {
                return 0;
            }
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
}
