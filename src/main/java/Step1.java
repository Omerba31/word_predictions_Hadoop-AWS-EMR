
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.FSDataOutputStream;
import java.net.URI;

import java.io.OutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class Step1 {

    private final static Set<String> stopWords = new HashSet<>();

    /**
     * Mapper class:
     * Processes lines to extract n-grams (only 1-grams, 2-grams, and 3-grams), filters out stop words, and emits n-grams with their occurrences.
     */

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            super.setup(context);
            Collections.addAll(stopWords, Config.stopWordsArr);
        }

        private boolean isValidToken(String token) {
            return token.matches("[א-ת]+");
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
            boolean containsNonHebrew = false;
            for (String word : words) {
                if (stopWords.contains(word)) {
                    containsStopWord = true;
                    break;
                }
                if (!isValidToken(word)) {
                    containsNonHebrew = true;
                    break;
                }
            }

            Text newKey;
            IntWritable newValue = new IntWritable(Integer.parseInt(occurrences));

            if (!containsStopWord && !containsNonHebrew) {
                // Construct keys based on n-gram length
                // 1-gram
                if (words.length == 1) {
                    // C1 and N1
                    newKey = new Text(words[0]);
                    context.write(newKey, newValue);
                    // C0
                    newKey = new Text("**"); // total occurrences
                    context.write(newKey, newValue);

                } // 2-gram
                else if (words.length == 2) {
                    // N2 and C2
                    newValue = new IntWritable(Integer.parseInt(occurrences));

                    newKey = new Text(words[1] + " " + words[0]);
                    context.write(newKey, newValue);

                    newKey = new Text(words[1] + " " + words[0] + " " + "**"); //checks if the pair is essential in the reducer
                    context.write(newKey, newValue);

                } // 3-gram
                else {
                    // N3
                    //Flip the order so it will send to the same reducer and add * in order it comes after all the other to the reducer.
                    newKey = new Text(words[1] + " " + words[0] + " " + words[2]);
                    context.write(newKey, newValue);
                }
            }
        }
    }

    /**
     * Reducer class:
     * Aggregates occurrences for each n-gram and prioritizes placeholders ().
     */

    public static class Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) sum += value.get();

            context.write(key, new IntWritable(sum));
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
        private long c0_Occurrences = 0L;

        private int c1_Occurrences = 0;
        private int n2_Occurrences = 0;
        private int n3_Occurrences = 0;

        private String currentOneGram = "";
        private String currentBiGram = "";
        private String currentTriGram = "";


        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            Text newKey, newValue;

            String keyString = key.toString();
            String[] keyWords = keyString.split("\\s+");

            if (keyString.equals("**")) {
                for (IntWritable value : values){
                    c0_Occurrences += value.get(); // C0 case
                }

            } else if (keyWords.length == 1) { // C1 and N1 case
                if (currentOneGram.isEmpty() || !(currentOneGram.equals(keyString))) {
                    c1_Occurrences = 0;
                    currentOneGram = keyString;
                }

                for (IntWritable value : values) c1_Occurrences += value.get();

            } else if (keyWords.length == 2) {// N2 case
                if (currentBiGram.isEmpty() || !(currentBiGram.equals(keyString))) {
                    n2_Occurrences = 0;
                    currentBiGram = keyString;
                }

                for (IntWritable value : values) n2_Occurrences += value.get();

            } else if (keyWords.length == 3) {// C2
                if (currentTriGram.isEmpty() || !(currentTriGram.equals(keyString))) {
                    n3_Occurrences = 0;
                    currentTriGram = keyString;
                }

                for (IntWritable value : values) n3_Occurrences += value.get();

                if (keyWords[2].equals("**")) {
                    newKey = new Text(keyWords[0] + " " + keyWords[1]); // N2_Key

                    int N2_Value = n2_Occurrences, N1_Value = c1_Occurrences;
                    newValue = new Text(N1_Value + " " + N2_Value);

                    context.write(newKey, newValue);

                } else {
                    // Reorder w2,w1,w3 -> w3,w2,w1
                    newKey = new Text(keyWords[2] + " " + keyWords[0] + " " + keyWords[1]); // N3_Key
                    int N3_Value = n3_Occurrences, C2_Value = n2_Occurrences, C1_Value = c1_Occurrences;
                    newValue = new Text(N3_Value + " " + C1_Value + " " + C2_Value);

                    context.write(newKey, newValue);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            FileSystem fs = Config.PATH_C0.getFileSystem(conf);

            // Optional: only one reducer should write this
            if (context.getTaskAttemptID().getTaskID().getId() == 0) {
                FSDataOutputStream out = fs.create(Config.PATH_C0, true); // overwrite = true
                out.writeBytes(String.valueOf(c0_Occurrences)); // write only the numeric value
                out.close();
                System.out.println("[INFO] Wrote C0 = " + c0_Occurrences + " to " + Config.PATH_C0);
            }
        }
    }


    /**
     * Partitioner class:
     * Distributes n-grams to reducers based on hash code.
     */

    public static class Partition extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            String keyString = key.toString();

            if (keyString.equals("**")) return 0;

            String[] parts = keyString.split("\\s+");
            String firstWord = parts.length >= 1 ? parts[0] : keyString;

            return Math.abs(firstWord.hashCode() % numPartitions);
        }
    }

    public static void main(String[] args) throws Exception {

        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 1 - NGram Count");

        job.setJarByClass(Step1.class);

        // Set the Mapper class
        job.setMapperClass(Map.class);

        // Specify Mapper output key/value types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Set the Combiner class (optional but improves performance)
        job.setCombinerClass(Step1.Combiner.class);

        // Set the Reducer class
        job.setReducerClass(Reduce.class);

        // Specify final output key/value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set custom Partitioner (based on the first word in the n-gram key)
        job.setPartitionerClass(Partition.class);

        // Define input and output formats
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job, Config.PATH_1_GRAM);
        SequenceFileInputFormat.addInputPath(job, Config.PATH_2_GRAM);
        SequenceFileInputFormat.addInputPath(job, Config.PATH_3_GRAM);

        long splitSize = 818089920L; // Set Split size to ~780MB -> 6 mappers
//        long splitSize = 1673031246L; // Set Split size to ~1.56GB -> 3 mappers

        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", splitSize);
        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.minsize", splitSize);

        // Set output path
        TextOutputFormat.setOutputPath(job, Config.OUTPUT_STEP_1);

        // Launch the job and exit based on its success
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}