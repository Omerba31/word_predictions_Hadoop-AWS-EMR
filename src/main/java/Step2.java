import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step2 {

    public static long C0 = 0 ;

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

//        @Override
//        protected void setup(Context context) throws IOException {
//            FileSystem fs = FileSystem.get(context.getConfiguration());
//            Path path = new Path("s3://dsp-02-bucket/vars/C0.txt");
//            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)))) {
//                Step2.C0 = Long.parseLong(br.readLine());
//            }
//        }

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString();
                String[] parts = line.split("\t");

                if (parts.length == 1) {
                    // This is the C0 line, accumulate total word count
                    C0 += Long.parseLong(parts[0]);
                } else {
                    // Forward the key-value pair as-is to the reducer
                    String keyPart = parts[0].trim();
                    String valuePart = parts[1].trim();
                    context.write(new Text(keyPart), new Text(valuePart));
                }
            }
        }

//    public static class Combiner extends Reducer<Text, Text, Text, Text> {
//        @Override
//        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            long sum = 0;
//            for (Text value : values) {
//                sum += Long.parseLong(value.toString());
//            }
//            context.write(key, new Text(String.valueOf(sum)));
//        }
//    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private String N1 = null;
        private String N2 = null;
        List<Text> combinedValues = new ArrayList<>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String keyString = key.toString();
            String[] keyParts = keyString.split("\\s+");

            if (keyParts.length == 2) {
                // Value string contains: N1, N2
                N1 = values.iterator().next().toString();
                N2 = values.iterator().next().toString();

            } else {
                // This is a trigram (w1,w2,w3)
                // Value string contains: N3, C1, C2

                // Create a list to include N1, N2, and existing values
                combinedValues.clear();

                // Add N1 and N2 as Text objects
                combinedValues.add(new Text(N1));
                combinedValues.add(new Text(N2));

                // Add the Value string that contains: N3, C1, C2 to combinedValues
                values.forEach(combinedValues::add);

                // Process the combined values with an iterator
                Iterator<Text> iterator = combinedValues.iterator();
                double probability = getProbability(iterator);
                // Reorder w3,w2,w1 -> w1,w2,w3 and emit with probability
                String newKey = keyParts[2] + " " + keyParts[1] + " " + keyParts[0];
                context.write((new Text( newKey)), new Text(String.valueOf(probability)));
            }
        }

        private double getProbability(Iterator<Text> iterator) {
            long N1 = Long.parseLong(iterator.next().toString());
            long N2 = Long.parseLong(iterator.next().toString());
            long N3 = Long.parseLong(iterator.next().toString());
            long C1 = Long.parseLong(iterator.next().toString());
            long C2 = Long.parseLong(iterator.next().toString());

            // Calculate weights k2 and k3
            double k2 = (Math.log(N2 + 1) + 1) / (Math.log(N2 + 1) + 2);  // k2 = log(N2 + 1) + 1 / (log(N2 + 1) + 2)
            double k3 = (Math.log(N3 + 1) + 1) / (Math.log(N3 + 1) + 2);  // k3 = log(N3 + 1) + 1 / (log(N3 + 1) + 2)

            // Calculate the probability components
            double P1 = (double) N3 / C2;      // P(w3 | w1, w2)
            double P2 = (double) N2 / C1;      // P(w3 | w2)
            double P3 = (double) N1 / C0; // P(w3)

            // Combine the components using the Thede & Harper formula
            return k3 * P1 + (1 - k3) * k2 * P2 + (1 - k3) * (1 - k2) * P3;
        }
    }

    public static class Partition extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String keyString = key.toString();
            String[] parts = keyString.split("\\s+");
            String firstWord = parts.length >= 1 ? parts[0] : keyString;

            return Math.abs(firstWord.hashCode() % numPartitions);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 2 - Interpolated Probability");

        job.setJarByClass(Step2.class);

        // Mapper class
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Reducer class
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Custom Partitioner (by first word)
        job.setPartitionerClass(Partition.class);

        // Input/output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Input and output paths
        TextInputFormat.addInputPath(job, Config.OUTPUT_STEP_1);
        TextOutputFormat.setOutputPath(job, Config.OUTPUT_STEP_2);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}