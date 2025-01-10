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
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.io.OutputStream;
import java.util.Set;
import java.util.HashSet;

public class Step1 {

    // The real path for the 1-gram, 2-gram, and 3-gram data files
//    public static final String INPUT_PATH_1GRAM = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data";
//    public static final String INPUT_PATH_2GRAM = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";
//    public static final String INPUT_PATH_3GRAM = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data";

    public static final String INPUT_PATH_1GRAM = "s3://dsp-02-bucket/grams/1gram";
    public static final String INPUT_PATH_2GRAM = "s3://dsp-02-bucket/grams/2gram";
    public static final String INPUT_PATH_3GRAM = "s3://dsp-02-bucket/grams/3gram";
    public static final String OUTPUT_STEP1_PATH = "s3://dsp-02-bucket/output_step_1/";

    private static Set<String> stopWords = new HashSet<>();

    /**
     * Mapper class:
     * Processes lines to extract n-grams (only 1-grams, 2-grams, and 3-grams), filters out stop words, and emits n-grams with their occurrences.
     */

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
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
            for (String word : stopWordsArr) {
                stopWords.add(word);
            }

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
                if (stopWords.contains(word)) {
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
                    // C0
                    context.write(new Text(String.format("%s", totalOccurrences)), new Text(String.format("%s", occurrences)));

                } // 2-gram
                else if (words.length == 2) {
                    // N2 and C2
                    keyString = String.format("<%s, %s>", words[0], words[1]);
                    context.write(new Text(keyString), new Text(occurrences));

//                    // C2
//                    //Flip the order so it will send to the same reducer and add * to mark it.
//                    keyString = String.format("<%s, %s, **>", words[1], words[0]);
//                    context.write(new Text(keyString), new Text(occurrences));

                } // 3-gram
                else {
                    // N3
                    //Flip the order so it will send to the same reducer and add * in order it come after all the other to the reducer.
                    keyString = String.format("<%s, %s, %s>", words[1], words[0], words[2]);
                    String dummyKeyString = String.format("<%s, %s, **>", words[2], words[1]);//checks if the pair is essential in the reducer
                    context.write(new Text(keyString), new Text(occurrences));
                    context.write(new Text(dummyKeyString), new Text(occurrences));
                }
            }
        }
    }

    /**
     * Reducer class:
     * Aggregates occurrences for each n-gram and prioritizes placeholders ().
     */

    public static class Combiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (Text value : values) {
                sum += Long.parseLong(value.toString());
            }
            context.write(key, new Text(String.valueOf(sum)));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private long c0_Occurrences = 0L;
        private long c1_Occurrences = 0L;
        private long n2_Occurrences = 0L;
        private long n3_Occurrences = 0L;
        private String currentOnegram = "";
        private String currentPair = "";
        private String currentTrigram = "";

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String keyString = key.toString();
            String[] keyWords = keyString.substring(1, keyString.length() - 1).split(",\\s*"); // Extract words from key

            if (key.toString().equals("**")) {
                for (Text value : values) {// C0 case
                    c0_Occurrences += Long.parseLong(value.toString());
                }
            } else if (keyWords.length == 1) {
                if (currentOnegram.isEmpty() || !(currentOnegram.equals(keyString))) {
                    c1_Occurrences = 0L;
                    currentOnegram = keyString;
                }
                for (Text value : values) {// C1 and N1 case
                    c1_Occurrences += Long.parseLong(value.toString());
                }
            } else if (keyWords.length == 2) {// N2 case
                if (currentPair.isEmpty() || !(currentPair.equals(keyString))) {
                    n2_Occurrences = 0L;
                    currentPair = keyString;
                }
                for (Text value : values) {
                    n2_Occurrences += Long.parseLong(value.toString());
                }
            } else if (keyWords.length == 3) {//C2
                if (currentTrigram.isEmpty() || !(currentTrigram.equals(keyString))) {
                    n3_Occurrences = 0L;
                    currentTrigram = keyString;
                }
                for (Text value : values) {
                    n3_Occurrences += Long.parseLong(value.toString());
                }
                currentTrigram = key.toString();
                currentTrigram = currentTrigram.substring(1, currentTrigram.length() - 1);// Remove the "<" and ">
                String[] words = currentTrigram.split(", ");
                if (words[2] == "**") {
                    String N2_Key = String.format("<%s, %s>", words[1], words[0]);
                    String N2_Value = Long.toString(n2_Occurrences);
                    String N1_Value = Long.toString(c1_Occurrences);
                    context.write(new Text(N2_Key), new Text(String.format("%s %s", N1_Value, N2_Value)));
                } else {
                    String N3_Key = String.format("<%s, %s, %s>", words[1], words[0], words[2]);
                    String N3_Value = Long.toString(n3_Occurrences);
                    String C2_Value = Long.toString(n2_Occurrences);
                    String C1_Value = Long.toString(c1_Occurrences);
                    context.write(new Text(N3_Key), new Text(String.format("%s %s %s", N3_Value, C1_Value, C2_Value)));
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Push the total sum of words to S3 after the reducer has finished
            if (!(c0_Occurrences == 0L)) {
                // Initialize counters for occurrences of each type
                String totalOccurrences = Long.toString(c0_Occurrences);
                FileSystem fs = FileSystem.get(context.getConfiguration());
                try (OutputStream out = fs.create(new Path("s3://dsp-02-bucket/vars/C0.txt"))) { // Write to S3
                    out.write(String.valueOf(totalOccurrences).getBytes());
                }
                super.cleanup(context);  // Ensure proper cleanup
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
        job.setCombinerClass(Step1.Combiner.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setCombinerClass(Step1.Combiner.class);//TODO: check combiner
        TextInputFormat.addInputPath(job, new Path(INPUT_PATH_1GRAM));
        TextInputFormat.addInputPath(job, new Path(INPUT_PATH_2GRAM));
        TextInputFormat.addInputPath(job, new Path(INPUT_PATH_3GRAM));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_STEP1_PATH));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}