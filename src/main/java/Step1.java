import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;


public class Step1 {

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
                String totalOccurrences = "<>";
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
                    keyString = String.format("<%s, **, %s, %s>", words[1], words[0], words[2]);
                    context.write(new Text(keyString), new Text(occurrences));
                }
            }
        }
    }
        /**
         * Reducer class:
         * Aggregates occurrences for each n-gram and prioritizes placeholders ().
         */

        public static class Reduce extends Reducer<Text, Text, Text, Text> {
            private long c0_Occurrences = 0L;
            private long c1_Occurrences = 0L;
            private long c2_Occurrences = 0L;
            private long n2_Occurrences = 0L;
            private long n3_Occurrences = 0L;
            private String currentWord = "";
            private String currentPair = "";
            private String currentSwitchedPair = "";
            private String currentTrigram = "";
            HashMap<String, String> pairs = new HashMap<>();

            @Override
            protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

                String keyString = key.toString();
                String[] keyWords = keyString.substring(1, keyString.length() - 1).split(",\\s*"); // Extract words from key

                if (key.toString().equals("")) {
                    for (Text value : values) {// C0 case
                        c0_Occurrences += Long.parseLong(value.toString());
                    }
                } else if (keyWords.length == 1) {
                    if (currentWord.isEmpty()) {
                        currentWord = key.toString();
                    }
                    if (!(currentWord.equals(key.toString()))) {
                        context.write(new Text(currentWord), new Text(String.format("%s", c1_Occurrences)));//N1
                        pairs.clear();
                        c1_Occurrences = 0;
                        n2_Occurrences = 0;
                        c2_Occurrences = 0;
                        n3_Occurrences = 0;
                        currentWord = key.toString();
                    }
                    for (Text value : values) {// C1 and N1 case
                        c1_Occurrences += Long.parseLong(value.toString());
                    }
                } else if (keyWords.length == 2) {// N2 case
                    if (!(currentPair.equals(key.toString()))) {
                        n2_Occurrences = 0;
                        currentPair = key.toString();
                    }
                    for (Text value : values) {
                        n2_Occurrences += Long.parseLong(value.toString());
                    }
                    pairs.put(currentPair, Long.toString(n2_Occurrences));
                } else if (keyWords.length == 3) {//C2
                    if (!(currentSwitchedPair.equals(key.toString()))) {
                        c2_Occurrences = 0;
                        currentSwitchedPair = key.toString();
                    }
                    for (Text value : values) {
                        c2_Occurrences += Long.parseLong(value.toString());
                    }
                    currentSwitchedPair = currentSwitchedPair.substring(1, currentSwitchedPair.length() - 4);// Remove "<" and " **>"
                    String[] words = currentSwitchedPair.split(", "); // This splits into words[1] and words[0]
                    String keyStringPair = String.format("<%s, %s>", words[1], words[0]);
                    pairs.put(keyStringPair, Long.toString(c2_Occurrences));
                } else if (keyWords.length == 4) {
                    if (!(currentTrigram.equals(key.toString()))) {
                        currentTrigram = key.toString();
                    }
                    for (Text value : values) {
                        n3_Occurrences += Long.parseLong(value.toString());
                    }
                    currentTrigram = currentTrigram.substring(1, currentTrigram.length() - 1);// Remove the "<" and ">
                    String[] words = currentTrigram.split(", ");
                    String keyStringTrigram = String.format("<%s, %s, %s>", words[1], words[0], words[2]);
                    ;
                    String C2_Key = String.format("<%s, %s>", words[1], words[0]);
                    String N2_Key = String.format("<%s, %s>", words[0], words[2]);
                    String C2_Value = pairs.get(C2_Key);
                    String N2_Value = pairs.get(N2_Key);
                    context.write(new Text(keyStringTrigram), new Text(String.format("%s %s %s %s", c1_Occurrences, C2_Value, N2_Value, n3_Occurrences)));
                }
            }

            @Override
            protected void cleanup(Context context) throws IOException, InterruptedException {
                // Push the total sum of words to S3 after the reducer has finished
                if (!(c0_Occurrences == 0L)) {
                    // Initialize counters for occurrences of each type
                    String totalOccurrences = Long.toString(c0_Occurrences);
                    //pushDataToS3(totalOccurrences);// TODO: fix the S3 push
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
    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step1.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(Step1.Partition.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setCombinerClass(Step1.Combiner.class);//TODO: Add combiner
        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data"));
        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data"));
        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));
        FileOutputFormat.setOutputPath(job, new Path("s3://bucket163897429777/output_step_11"));//TODO: Fix with correct path
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}