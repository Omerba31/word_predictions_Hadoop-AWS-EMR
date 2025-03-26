import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

public class Step2 {


    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] fields = value.toString().split("\t");

            if (fields.length != 2 && fields.length != 3) {
                System.err.println("[ERROR] Invalid input format: " + value.toString());
                return;
            }

            // Forward the key-value pair as-is to the reducer
            Text newKey = new Text(fields[0].trim()), newValue = new Text(fields[1].trim());
            context.write(newKey, newValue);

        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        String[] newValues; //NEW
        private long C0 = 1L; // fallback default

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Path path = Config.PATH_C0;
            FileSystem fs = path.getFileSystem(conf);

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                String line = reader.readLine();
                if (line != null && !line.isEmpty()) {
                    C0 = Long.parseLong(line.trim()); // Assume file contains just a number
                    System.out.println("[INFO] Loaded C0 from S3 = " + C0);
                }
            } catch (Exception e) {
                System.err.println("[ERROR] Failed to read C0 from S3: " + e.getMessage());
                throw e;
            }
        }

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

                        double probability = getProbability(newValues, C0);
                        newValue = new Text(String.valueOf(probability));

                        newValues = new String[5]; // Reset for next iteration // NEW

                        context.write(newKey, newValue);
                    }
                }
            }
        }

        private double getProbability(String[] valueParts, long C0) {
            for (String value : valueParts) {
                if (value == null || value.isEmpty()) {
                    System.err.println("[ERROR] Value is null");
                    return 0.0;
                }
            }

            double N1 = Long.parseLong(valueParts[0]);
            double N2 = Long.parseLong(valueParts[1]);
            double N3 = Long.parseLong(valueParts[2]);
            double C1 = Long.parseLong(valueParts[3]);
            double C2 = Long.parseLong(valueParts[4]);

//            long C0 = getFromS3(context, Config.PATH_C0);

            if (C2 == 0 | C1 == 0 | C0 == 0) {
                System.err.println("[ERROR] C2, C1 or C0 is zero");
                return 0.0;
            }

            // Calculate weights k2 and k3
            double k2 = ((Math.log(N2 + 1) + 1) / (Math.log(N2 + 1) + 2));
            double k3 = ((Math.log(N3 + 1) + 1) / (Math.log(N3 + 1) + 2));

            // Calculate the probability components
            double P1 = N3 / C2; // P(w3 | w1, w2)
            double P2 = N2 / C1; // P(w3 | w2)
            double P3 = N1 / C0; // P(w3)


//            double P3 = Math.min(1.0, N1 / C0);

            // Combine the components using the Thede & Harper formula
            return  ((k3 * P1) + ((1 - k3) * k2 * P2) + ((1 - k3) * (1 - k2) * P3));

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