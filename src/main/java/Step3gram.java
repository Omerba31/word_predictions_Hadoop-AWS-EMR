import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class Step3gram {

    /**
     * Mapper class:
     * Processes each line, filters out stop words, and emits tri-grams with their occurrences.
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
            String threeGram = fields[0].trim() + " " + fields[1].trim() + " " + fields[2].trim(); // First 3 fields are the 3-gram
            String occurrences = fields[4].trim(); // 5th field is the occurrences

            // Check if all words in the three-gram are not stop words
            String[] words = threeGram.split(" ");
            if (words.length == 3 && !stopWords.containsKey(words[0]) && !stopWords.containsKey(words[1]) && !stopWords.containsKey(words[2])) {
                context.write(new Text(threeGram), new Text(occurrences));
            }
        }
    }

    /**
     * Reducer class:
     * Aggregates occurrences for each 3gram.
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long sumOccurrences = 0L;
            for (Text value : values) {
                try {
                    sumOccurrences += Long.parseLong(value.toString());
                } catch (NumberFormatException e) {
                    context.getCounter("Step3gram", "InvalidOccurrences").increment(1);
                }
            }
            context.write(key, new Text(Long.toString(sumOccurrences)));
        }
    }

    /**
     * Partitioner class:
     * Distributes 3grams to reducers based on hash code.
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
