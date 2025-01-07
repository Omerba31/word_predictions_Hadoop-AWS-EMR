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
import java.util.Objects;


public class step_1 {

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
                String keyString;
                String totalOccurrences = "<**>";
                // 1-gram
                if (words.length == 1) {
                    // C1 and N1
                    keyString = String.format("<%s>", words[0]);
                    context.write(new Text(String.format("%s", keyString)), new Text(String.format("%s", occurrences)));
                    //C0
                    context.write(new Text(String.format("%s", totalOccurrences)), new Text(String.format("%s", occurrences)));

                } // 2-gram
                else if (words.length == 2) {
                    // N2
                    keyString = String.format("<%s, %s>", words[0], words[1]);
                    context.write(new Text(keyString), new Text(occurrences));

                    // C2
                    //Flip the order so it will send to the same reducer and add * to mark it.
                    keyString = String.format("<%s, %s, **>", words[1], words[0]);
                    context.write(new Text(keyString), new Text(occurrences));

                } // 3-gram
                else {
                    // N3
                    //Flip the order so it will send to the same reducer and add * in order it come after all the other to the reducer.
                    keyString = String.format("<%s, %s, %s, **>", words[1], words[0], words[2]);
                    context.write(new Text(keyString), new Text(occurrences));
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
                String[] keyWords = keyString.substring(1, keyString.length() - 1).split(",\\s*"); // Extract words from key

                // Iterate over the values to aggregate the counts
                for (Text value : values) {
                    try {
                        long occurrences = Long.parseLong(value.toString());

                        if (keyString.equals("<**>")) {
                            c0_Occurrences += occurrences;
                            // C0 case, sum occurrences based on the whole n-gram

                        } else if (keyWords.length == 3) { // Handle cases for 3-grams

                            if (keyWords[0].equals("**") && !keyWords[1].equals("**") && keyWords[2].equals("**")) {
                                c1_Occurrences += occurrences;
                                // C1 case, sum occurrences based on the second word

                            } else if (keyWords[0].equals("**") && keyWords[1].equals("**") && !keyWords[2].equals("**")) {
                                n1_Occurrences += occurrences;
                                // N1 case, sum occurrences based on the third word

                            } else if (!keyWords[0].equals("**") && !keyWords[1].equals("**") && keyWords[2].equals("**")) {
                                c2_Occurrences += occurrences;
                                // C2 case, sum occurrences based on the first pair words

                            } else if (keyWords[0].equals("**") && !keyWords[1].equals("**") && !keyWords[2].equals("**")) {
                                n2_Occurrences += occurrences;
                                // N2 case, sum occurrences based on the second pair words

                            } else if (!keyWords[0].equals("**") && !keyWords[1].equals("**") && !keyWords[2].equals("**")) {
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
                if (keyString.equals("<**, **, **>")) {
                    context.write(key, new Text(String.valueOf(c0_Occurrences)));
                }

                // For C1, C2, N2, N3, return the sum of occurrences per variable
                else if (n1_Occurrences == 0) { //
                    String outputValue =
                            "C1 occurrences=" + c1_Occurrences + ", " +
                                    "C2 occurrences=" + c2_Occurrences + ", " +
                                    "N2 occurrences=" + n2_Occurrences + ", " +
                                    "N3 occurrences=" + n3_Occurrences;

                    context.write(new Text(outPutKeyString), new Text(outputValue));
                }

                // For N1, return the sum of occurrences as a single output value
                else {
                    context.write(key, new Text(String.valueOf(n1_Occurrences)));
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
                return Math.abs(key.hashCode() % numPartitions);
            }
        }
    }
}