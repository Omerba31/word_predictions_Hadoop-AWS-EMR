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

public class StepGrams {

    /**
     * Mapper class:
     * Processes lines to extract n-grams (only 1-grams, 2-grams, and 3-grams), filters out stop words, and emits n-grams with their occurrences.
     */
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private final HashMap<String, Integer> stopWords = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            Main.generateStopWord(context, stopWords);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            String nGram = fields[0].trim(); // The whole n-gram is in the first field
            String occurrences = fields[fields.length - 1].trim(); // Last field contains occurrences

            // Check if all words in the n-gram are not stop words
            String[] words = nGram.split(" ");
            if (words.length > 3 || words.length < 1) {
                // Skip n-grams that are not 1-gram, 2-gram, or 3-gram
                return;
            }

            boolean containsStopWord = false;
            for (String word : words) {
                if (stopWords.containsKey(word)) {
                    containsStopWord = true;
                    break;
                }
            }

            if (!containsStopWord) {
                // Construct keys based on n-gram length
                String key;
                if (words.length == 1) {
                    key = "<**, **, " + words[0] + ">";
                } else if (words.length == 2) {
                    key = "<**, " + words[0] + ", " + words[1] + ">";
                } else { // words.length == 3
                    key = "<" + words[0] + ", " + words[1] + ", " + words[2] + ">";
                }

                context.write(new Text(key), new Text(occurrences));
            }
        }
    }

    /**
     * Reducer class:
     * Aggregates occurrences for each n-gram and prioritizes placeholders (**).
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long sumOccurrences = 0L;
            for (Text value : values) {
                try {
                    sumOccurrences += Long.parseLong(value.toString());
                } catch (NumberFormatException e) {
                    context.getCounter("StepGrams", "InvalidOccurrences").increment(1);
                }
            }
            // Ensure keys with ** come before others by organizing output if necessary
            context.write(key, new Text(Long.toString(sumOccurrences)));
        }
    }

    /**
     * Partitioner class:
     * Distributes n-grams to reducers based on hash code.
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

    /**
     * Comparator class:
     * Custom comparator to sort n-grams based on placeholder order and lexical order.
     */
    public static class NGramComparator extends WritableComparator {
        protected NGramComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            String key1 = a.toString();
            String key2 = b.toString();

            // Extract the structure: number of placeholders and word content
            int score1 = calculateScore(key1);
            int score2 = calculateScore(key2);

            if (score1 != score2) {
                return Integer.compare(score1, score2); // Lower score comes first
            }

            // If scores are equal, compare lexically
            return key1.compareTo(key2);
        }

        private int calculateScore(String key) {
            String[] parts = key.replaceAll("[<>,]", "").split(" ");
            int score = 0;
            for (String part : parts) {
                if ("**".equals(part)) {
                    score++; // Higher score for placeholders
                }
            }
            return score;
        }
    }
}
