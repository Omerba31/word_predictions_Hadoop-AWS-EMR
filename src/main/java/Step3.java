import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Step3 {

    private static class Map extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().trim().split("\t");
            if (parts.length != 2) return;

            String trigram = parts[0];
            String probability = parts[1];

            Text compositeKey = new Text(trigram + " " + probability);
            context.write(compositeKey, new Text(""));
        }
        }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] parts = key.toString().split("\\s+");
            if (parts.length < 4) return;

            String trigram = parts[0] + " " + parts[1] + " " + parts[2];
            String prob = parts[3];

            context.write(new Text(trigram), new Text(prob));
        }
    }

    public static class Partition extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String keyString = key.toString();
            String[] parts = keyString.split("\\s+");

            // Extract w1 and w2 (first and second word)
            String w1w2 = parts.length >= 2 ? parts[0] + " " + parts[1] : keyString;

            return Math.abs(w1w2.hashCode() % numPartitions);
        }
    }

    private static class Comparison extends WritableComparator {
        protected Comparison() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable key1, WritableComparable key2) {
            String[] parts1 = key1.toString().split("\\s+");
            String[] parts2 = key2.toString().split("\\s+");

            // Compare by w1 + w2
            String w1w2_1 = parts1[0] + " " + parts1[1];
            String w1w2_2 = parts2[0] + " " + parts2[1];
            int cmp = w1w2_1.compareTo(w1w2_2);
            if (cmp != 0) return cmp;

            // Compare by probability DESC
            double prob1 = Double.parseDouble(parts1[3]); // last part is probability
            double prob2 = Double.parseDouble(parts2[3]);
            return Double.compare(prob2, prob1);
        }
    }


    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(Step3.class);

        // Mapper class
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Reducer class
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Custom Partitioner / Comparator
        job.setPartitionerClass(Step3.Partition.class);
        job.setSortComparatorClass(Step3.Comparison.class);

        job.setNumReduceTasks(1);

        // Input/output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Input and output paths
        TextInputFormat.addInputPath(job, Config.OUTPUT_STEP_2);
        TextOutputFormat.setOutputPath(job, Config.OUTPUT_STEP_3);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}