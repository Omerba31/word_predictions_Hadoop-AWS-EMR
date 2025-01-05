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


public class step_grams {

    public static void generateStopWord(Mapper<LongWritable, Text, Text, Text>.Context context, HashMap<String, Integer> stopWords) throws IOException {
        Path stopWordsPath = new Path("src/resource/heb-stopwords.txt");
        FileSystem fs = FileSystem.getLocal(context.getConfiguration());
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(stopWordsPath)))) {
            String word;
            while ((word = br.readLine()) != null) {
                stopWords.put(word.trim(), 1);
            }
        }
    }

    /**
     * Mapper class:
     * Processes lines to extract n-grams (only 1-grams, 2-grams, and 3-grams), filters out stop words, and emits n-grams with their occurrences.
     */
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private final HashMap<String, Integer> stopWords = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            generateStopWord(context, stopWords);
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
                String keyString1, keyString2, keyString3;
                if (words.length == 1) {
                    keyString1 = "<, **, **>";
                    keyString2 = "<, " + words[0] + ", **>";
                    keyString3 = "<, **, " + words[0] + ">";
                    context.write(new Text(keyString1), new Text(occurrences)); //C0
                    context.write(new Text(keyString2), new Text(occurrences)); //C1
                    context.write(new Text(keyString3), new Text(occurrences));//N1

                } else if (words.length == 2) {
                    keyString1 = "<" + words[0] + ", " + words[1] + ", **>";
                    keyString2 = "<, " + words[0] + ", " + words[1] + ">";
                    context.write(new Text(keyString1), new Text(occurrences));//C2
                    context.write(new Text(keyString2), new Text(occurrences));//N2
                } else { // words.length == 3
                    keyString1 = "<" + words[0] + ", " + words[1] + ", " + words[2] + ">";
                    context.write(new Text(keyString1), new Text(occurrences));//N3
                }
            }

        }

        /**
         * Reducer class:
         * Aggregates occurrences for each n-gram and prioritizes placeholders ().
         */
        public static class Reduce extends Reducer<Text, Text, Text, Text> {

            @Override
            protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                // Initialize counters for occurrences of each type
                long n1_Occurrences = 0L;
                long n2_Occurrences = 0L;
                long n3_Occurrences = 0L;
                long c0_Occurrences = 0L;
                long c1_Occurrences = 0L;
                long c2_Occurrences = 0L;
                String outPutKeyString = "";

                String keyString = key.toString();
                String[] words = keyString.substring(1, keyString.length() - 1).split(",\\s*");

                // Iterate over the values to aggregate the counts
                for (Text value : values) {
                    try {
                        long occurrences = Long.parseLong(value.toString());

                        if (keyString.equals("<, **, **>")) {
                            // For C0 (triple **), we sum all occurrences
                            c0_Occurrences += occurrences;
                        } else if (words.length == 3) {
                            // Handle cases for 3-grams
                            if (words[0].isEmpty() && !words[1].isEmpty() && words[2].isEmpty()) {
                                // C1 case, sum occurrences based on the second word
                                c1_Occurrences += occurrences;
                            } else if (words[0].isEmpty() && words[1].isEmpty() && !words[2].isEmpty()) {
                                // N1 case, sum occurrences based on the third word
                                n1_Occurrences += occurrences;
                            } else if (!words[0].isEmpty() && !words[1].isEmpty() && words[2].isEmpty()) {
                                // C2 case, sum occurrences based on the first pair words
                                c2_Occurrences += occurrences;
                            } else if (words[0].isEmpty() && !words[1].isEmpty() && !words[2].isEmpty()) {
                                // N2 case, sum occurrences based on the second pair words
                                n2_Occurrences += occurrences;
                            } else if (words[0].isEmpty() && words[1].isEmpty() && words[2].isEmpty()) {
                                // N3 case, sum occurrences based on the trigram words
                                n3_Occurrences += occurrences;
                                outPutKeyString = keyString; //Save the trigram key for output key
                            }
                        }
                    } catch (NumberFormatException e) {
                        context.getCounter("StepGrams", "InvalidOccurrences").increment(1);
                    }
                }

                // For C0, return the sum of occurrences as a single output value
                if (keyString.equals("<, **, **>")) {
                    context.write(key, new Text("sum=" + c0_Occurrences));
                }
                // For N2, C1, C2, N3, return the sum of occurrences per variable
                else if (n1_Occurrences == 0) {
                    String outputValue = "C1 occurrences=" + c1_Occurrences +
                            ", C2 occurrences=" + c2_Occurrences +
                            ", N2 occurrences=" + n2_Occurrences +
                            ", N3 occurrences=" + n3_Occurrences;

                    context.write(new Text(outPutKeyString), new Text(outputValue));
                }
                // For N1, return the sum of occurrences as a single output value
                else {
                    context.write(key, new Text("sum=" + n1_Occurrences));
                }
            }
        }


        /**
         * Partitioner class:
         * Distributes n-grams to reducers based on hash code.
         */
        public static class Partition extends Partitioner<Text, Text> {
            @Override
            public int getPartition(Text key, Text value, int numPartitions) {
                String keyStr = key.toString();
                String[] parts = keyStr.split(", ");

                // For C0 (triple **), send it to partition 0 (same reducer for all C0)
                if (keyStr.equals("<, **, **>")) {
                    return 0; // Always send C0 to partition 0
                }

                // For N1: Partition based on the third word when the second word is **
                if (parts.length == 3 && parts[1].isEmpty()) {
                    // Extract the third word (since the second part is **)
                    String thirdWord = parts[2].replaceAll("[<>]", "").trim();
                    return (thirdWord.hashCode() & Integer.MAX_VALUE) % numPartitions;
                }

                // For N2, C1, C2, N3: Partition based on the second word if it's not **
                if (parts.length == 3 && !parts[1].isEmpty()) {
                    // Extract the second word
                    String secondWord = parts[1].replaceAll("[<>]", "").trim();
                    return (secondWord.hashCode() & Integer.MAX_VALUE) % numPartitions;
                }

                // Default case (shouldn't be needed)
                return (keyStr.hashCode() & Integer.MAX_VALUE) % numPartitions;
            }
        }
    }
}