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

    public static long C0 = 0;

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
                Text newKey = new Text(parts[0].trim()), newValue = new Text(parts[1].trim());
                context.write(newKey, newValue);
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

        String[] newValues; //NEW

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String keyString = key.toString();
            String[] keyParts = keyString.split("\\s+");
            Text newKey, newValue;

            outerLoop:
            for (Text value : values) {

                String valueString = value.toString();
                String[] valueParts = valueString.split("\\s+");

                if (keyParts.length == 2) {
                    newValues = new String[5];

                    if (valueParts.length < 2)
                        System.err.println("[ERROR] Invalid value format: " + valueParts[0]);

                    else {
                        newValues[0] = valueParts[0];
                        newValues[1] = valueParts[1];
                    }

                } else {

                    // This is a trigram (w1,w2,w3)
                    // Value string contains: N3, C1, C2

                    if (valueParts.length < 3)
                        System.err.println("[ERROR] Invalid value format: " + valueParts[0]);

                    else {
                        newValues[2] = valueParts[0]; // N3
                        newValues[3] = valueParts[1]; // C1
                        newValues[4] = valueParts[2]; // C2

                        for (String _newValue : newValues)
                            if (_newValue == null) continue outerLoop; // option: change to return

                        // Reorder w3,w2,w1 -> w1,w2,w3 and emit with probability
                        newKey = new Text(keyParts[2] + " " + keyParts[1] + " " + keyParts[0]);

                        double probability = getProbability(newValues);
                        newValue = new Text(String.valueOf(probability));

                        newValues = new String[5]; // Reset for next iteration // NEW

                        context.write(newKey, newValue);
                    }
                }
            }
        }

        private double getProbability(String[] valueParts) {

            for (String value : valueParts) {
                if (value == null || value.isEmpty()) {
                    System.err.println("[ERROR] Value is null");
                    return 0.0;
                }
            }

            for (String value : valueParts) if (value.equals("0")) return 0.0;

            long N1 = Long.parseLong(valueParts[0]);
            long N2 = Long.parseLong(valueParts[1]);
            long N3 = Long.parseLong(valueParts[2]);
            long C1 = Long.parseLong(valueParts[3]);
            long C2 = Long.parseLong(valueParts[4]);

            // Calculate weights k2 and k3
            double k2 = (Math.log(N2 + 1) + 1) / (Math.log(N2 + 1) + 2);  // k2 = log(N2 + 1) + 1 / (log(N2 + 1) + 2)
            double k3 = (Math.log(N3 + 1) + 1) / (Math.log(N3 + 1) + 2);  // k3 = log(N3 + 1) + 1 / (log(N3 + 1) + 2)

            // Calculate the probability components
            double P1 = (double) N3 / C2; // P(w3 | w1, w2)
            double P2 = (double) N2 / C1; // P(w3 | w2)
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
            //TODO: should be by first and second word?
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