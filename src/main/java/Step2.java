import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step2 {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t");

            if (parts.length < 2) return;

            String[] ngram = parts[0].split(",");
            String occurrences = parts[1];

            // For single word (N1)
            if (ngram.length == 1) {
                context.write(new Text("<" + ngram[0] + ">"), new Text(occurrences));
            }

            // For 3-gram, reorder as w3,w1,w2
            else if (ngram.length == 3) {
                context.write(
                        new Text(String.format("<%s,%s,%s>", ngram[2], ngram[0], ngram[1])),
                        new Text(occurrences)
                );
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private long currentN1 = 0;  // Number of times w3 occurs

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String keyStr = key.toString().substring(1, key.getLength() - 1);
            String[] keyParts = keyStr.split(",");

            if (keyParts.length == 1) {
                // This is a single word (w3) count
                currentN1 = Long.parseLong(values.iterator().next().toString());

            } else {
                // This is a trigram (w1,w2,w3)
                // Value string contains: C1, C2, N2, N3
                Iterator<Text> iterator = values.iterator();
                double probability = getProbability(iterator);

                // Reorder w3,w1,w2 -> w1,w2,w3 and emit with probability
                context.write(
                        new Text(String.format("<%s,%s,%s>", keyParts[1], keyParts[2], keyParts[0])),
                        new Text(String.valueOf(probability))
                );
            }
        }

        private double getProbability(Iterator<Text> iterator) {
            //ORDER: C1, C2, N2, N3
            long C1 = Long.parseLong(iterator.next().toString());
            long C2 = Long.parseLong(iterator.next().toString());
            long N2 = Long.parseLong(iterator.next().toString());
            long N3 = Long.parseLong(iterator.next().toString());

            long C0 = 1;  // Total words in corpus, C0
            //TODO: get this from s3

            // Calculate weights k2 and k3
            double k2 = (Math.log(N2 + 1) + 1) / (Math.log(N2 + 1) + 2);  // k2 = log(N2 + 1) + 1 / (log(N2 + 1) + 2)
            double k3 = (Math.log(N3 + 1) + 1) / (Math.log(N3 + 1) + 2);  // k3 = log(N3 + 1) + 1 / (log(N3 + 1) + 2)

            // Calculate the probability components
            double P1 = (double) N3 / C2;      // P(w3 | w1, w2)
            double P2 = (double) N2 / C1;      // P(w3 | w2)
            double P3 = (double) currentN1 / C0; // P(w3)

            // Combine the components using the Thede & Harper formula
            return k3 * P1 + (1 - k3) * k2 * P2 + (1 - k3) * (1 - k2) * P3;
        }
    }

    public static class Partition extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs(key.hashCode() % numPartitions);
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step2.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(Step2.Partition.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setCombinerClass(Step2.Combiner.class);//TODO: Add combiner
//        FileInputFormat.addInputPath(job, new Path("/home/spl211/IdeaProjects/MapReduceProject/output_step_11/part-r-00000"));
        FileInputFormat.addInputPath(job, new Path("s3://bucket163897429777/output_step_11"));//TODO: Fix with correct path
//        FileOutputFormat.setOutputPath(job, new Path("/home/spl211/IdeaProjects/MapReduceProject/output_step_22"));
        FileOutputFormat.setOutputPath(job, new Path("s3://bucket163897429777/output_step_22"));//TODO: Fix with correct path
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}