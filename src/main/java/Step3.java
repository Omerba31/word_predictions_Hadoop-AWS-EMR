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
            String[] keyValue = value.toString().split("\t");
            if (keyValue.length != 2) return; // Skip malformed lines

            context.write(new Text(keyValue[0]), new Text(keyValue[1]));
        }
    }

    private static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Fetch the single value directly
            if (values.iterator().hasNext()) {
                context.write(key, values.iterator().next());
            }
        }
    }

    public static class Partition extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String keyString = key.toString();
            // Extract the first word inside the brackets
            String firstWord = keyString.substring(1, keyString.indexOf(",") > 0 ? keyString.indexOf(",") : keyString.length() - 1).trim();
            return Math.abs(firstWord.hashCode() % numPartitions);
        }
    }

    private static class Comparison extends WritableComparator {
        protected Comparison() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable key1, WritableComparable key2) {
            // Extract components: <w1,w2,w3>
            String[] parts1 = key1.toString().split("\\s+");
            String[] parts2 = key2.toString().split("\\s+");

            // Compare w1,w2 lexicographically
            int cmp = (parts1[0] + "," + parts1[1]).compareTo(parts2[0] + "," + parts2[1]);
            if (cmp != 0) return cmp; // If w1,w2 differ, sort lexicographically

            // If w1,w2 are the same, compare probabilities descending
            double prob1 = Double.parseDouble(parts1[2]);
            double prob2 = Double.parseDouble(parts2[2]);

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