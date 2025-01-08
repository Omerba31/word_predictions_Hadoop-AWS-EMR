import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

public class Step3 {

    private static class Map extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] keyValue = value.toString().split("\t");
            if (keyValue.length != 2)
                return; // Skip malformed lines

            // Output the key and value as is
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
            return Math.abs(key.hashCode() % numPartitions);
        }
    }

    private static class Comparison extends WritableComparator {
        protected Comparison() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable key1, WritableComparable key2) {
            // Extract components: <w1,w2,w3>
            String[] parts1 = key1.toString().substring(1, key1.toString().length() - 1).split(",");
            String[] parts2 = key2.toString().substring(1, key2.toString().length() - 1).split(",");

            // Compare w1,w2 lexicographically
            int cmp = (parts1[0] + "," + parts1[1]).compareTo(parts2[0] + "," + parts2[1]);
            if (cmp != 0) {
                return cmp; // If w1,w2 differ, sort lexicographically
            }

            // If w1,w2 are the same, compare probabilities descending
            double prob1 = Double.parseDouble(parts1[2]);
            double prob2 = Double.parseDouble(parts2[2]);
            return Double.compare(prob2, prob1);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step3.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setSortComparatorClass(Step3.Comparison.class);
        job.setReducerClass(Reduce.class);
        job.setPartitionerClass(Step3.Partition.class);
        job.setNumReduceTasks(1);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
//        FileInputFormat.addInputPath(job, new Path("/home/spl211/IdeaProjects/MapReduceProject/output_step_33/part-r-00000"));
        FileInputFormat.addInputPath(job, new Path("s3://bucket163897429777/output_step_33"));//TODO: Add correct path
//        FileOutputFormat.setOutputPath(job, new Path("/home/spl211/IdeaProjects/MapReduceProject/output_step_44"));
        FileOutputFormat.setOutputPath(job, new Path("s3://bucket163897429777/output_step_44"));//TODO: Add correct path
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}