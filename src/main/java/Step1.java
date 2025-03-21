
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
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
            String[] stopWordsArr = {
                    "״", "׳", "של", "רב", "פי", "עם", "עליו", "עליהם", "על", "עד", "מן", "מכל", "מי",
                    "מהם", "מה", "מ", "למה", "לכל", "לי", "לו", "להיות", "לה", "לא", "כן", "כמה", "כלי",
                    "כל", "כי", "יש", "ימים", "יותר", "יד", "י", "זה", "ז", "ועל", "ומי", "ולא", "וכן",
                    "וכל", "והיא", "והוא", "ואם", "ו", "הרבה", "הנה", "היו", "היה", "היא", "הזה", "הוא",
                    "דבר", "ד", "ג", "בני", "בכל", "בו", "בה", "בא", "את", "אשר", "אם", "אלה", "אל",
                    "אך", "איש", "אין", "אחת", "אחר", "אחד", "אז", "אותו", "־", "^", "?", ";", ":", "1",
                    ".", "-", "*", "\"", "!", "שלשה", "בעל", "פני", ")", "גדול", "שם", "עלי", "עולם",
                    "מקום", "לעולם", "לנו", "להם", "ישראל", "יודע", "זאת", "השמים", "הזאת", "הדברים",
                    "הדבר", "הבית", "האמת", "דברי", "במקום", "בהם", "אמרו", "אינם", "אחרי", "אותם",
                    "אדם", "(", "חלק", "שני", "שכל", "שאר", "ש", "ר", "פעמים", "נעשה", "ן", "ממנו",
                    "מלא", "מזה", "ם", "לפי", "ל", "כמו", "כבר", "כ", "זו", "ומה", "ולכל", "ובין",
                    "ואין", "הן", "היתה", "הא", "ה", "בל", "בין", "בזה", "ב", "אף", "אי", "אותה",
                    "או", "אבל", "א"
            };
            Collections.addAll(stopWords, stopWordsArr);
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
                    break;}
            }

            if (!containsStopWord && !containsNonHebrew){
                // Construct keys based on n-gram length
                String keyString;
                String totalOccurrences = "**";
                // 1-gram
                if (words.length == 1) {
                    // C1 and N1
                    keyString = words[0];
                    context.write(new Text(String.format(keyString)), new IntWritable(Integer.parseInt(occurrences)));
                    // C0
                    context.write(new Text(String.format(totalOccurrences)), new IntWritable(Integer.parseInt(occurrences)));

                } // 2-gram
                else if (words.length == 2) {
                    // N2 and C2
                    keyString = words[1] + " "  + words[0];
                    String dummyKeyString = words[1] + " "  + words[0] + " " + "**";//checks if the pair is essential in the reducer
                    context.write(new Text(keyString), new IntWritable(Integer.parseInt(occurrences)));
                    context.write(new Text(dummyKeyString), new IntWritable(Integer.parseInt(occurrences)));

                } // 3-gram
                else {
                    // N3
                    //Flip the order so it will send to the same reducer and add * in order it comes after all the other to the reducer.
                    keyString = words[1] + " " + words[0] + " " + words[2];
                    context.write(new Text(keyString), new IntWritable(Integer.parseInt(occurrences)));
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
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
        private int c0_Occurrences = 0;
        private int c1_Occurrences = 0;
        private int n2_Occurrences = 0;
        private int n3_Occurrences = 0;
        private String currentOneGram = "";
        private String currentBiGram = "";
        private String currentTriGram = "";

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            String keyString = key.toString();
            String[] keyWords = keyString.split("\\s+");

            if (keyString.equals("**")) {
                for (IntWritable value : values) {// C0 case
                    c0_Occurrences += value.get();
                    }
                    context.write(new Text("**"), new Text(Integer.toString(c0_Occurrences)));
            } else if (keyWords.length == 1) {
                if (currentOneGram.isEmpty() || !(currentOneGram.equals(keyString))) {
                    c1_Occurrences = 0;
                    currentOneGram = keyString;
                }
                for (IntWritable value : values) {// C1 and N1 case
                    c1_Occurrences += value.get();
                }

            } else if (keyWords.length == 2) {// N2 case
                if (currentBiGram.isEmpty() || !(currentBiGram.equals(keyString))) {
                    n2_Occurrences = 0;
                    currentBiGram = keyString;
                }
                for (IntWritable value : values) {
                    n2_Occurrences += value.get();
                }

            } else if (keyWords.length == 3) {// C2
                if (currentTriGram.isEmpty() || !(currentTriGram.equals(keyString))) {
                    n3_Occurrences = 0;
                    currentTriGram = keyString;
                }
                for (IntWritable value : values) {
                    n3_Occurrences += value.get();
                }

                if (keyWords[2].equals("**")) {
                    String N2_Key = keyWords[0] + " " + keyWords[1];
                    int N2_Value = n2_Occurrences;
                    int N1_Value = c1_Occurrences;
                    context.write(new Text(N2_Key), new Text(N1_Value + " " + N2_Value));
                } else {
                    String N3_Key = keyWords[2] + " " + keyWords[0] + " " + keyWords[1];
                    int N3_Value = n3_Occurrences;
                    int C2_Value = n2_Occurrences;
                    int C1_Value = c1_Occurrences;
                    context.write(new Text(N3_Key), new Text(N3_Value + " " + C1_Value + " " + C2_Value));
                }
            }
        }

//        @Override
//        protected void cleanup(Context context) throws IOException, InterruptedException {
//            // Push the total sum of words to S3 after the reducer has finished
//            if (!(c0_Occurrences == 0L)) {
//                // Initialize counters for occurrences of each type
//                String totalOccurrences = Long.toString(c0_Occurrences);
//                FileSystem fs = FileSystem.get(context.getConfiguration());
//                try (OutputStream out = fs.create(new Path("s3://dsp-02-bucket/vars/C0.txt"))) { // Write to S3
//                    out.write(totalOccurrences.getBytes());
//                }
//                super.cleanup(context);  // Ensure proper cleanup
//            }
//        }
    }

    /**
     * Partitioner class:
     * Distributes n-grams to reducers based on hash code.
     */

    public static class Partition extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            String keyString = key.toString();

            if (keyString.equals("**")) {
                return 0;
            }

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

        // Set output path
        TextOutputFormat.setOutputPath(job, Config.OUTPUT_STEP_1);

        // Launch the job and exit based on its success
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}