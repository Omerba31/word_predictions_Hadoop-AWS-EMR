import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ConditionalProbabilityCalculator {

    /**
     * Mapper class:
     * Prepares data for joining and further computation.
     */
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            String nGram = fields[0].trim(); // Entire n-gram
            String occurrences = fields[1].trim(); // Count of the n-gram
            String[] words = nGram.split(" "); // Split n-gram into individual words

            if (words.length == 1) {
                // For 1-gram
                context.write(new Text(words[0]), new Text("N1=" + occurrences));
                // Emit C0 for total word occurrences
                context.write(new Text("C0"), new Text("C0=" + occurrences));
            } else if (words.length == 2) {
                // For 2-gram
                context.write(new Text(words[1]), new Text("N2=" + occurrences));
                context.write(new Text(words[0] + " " + words[1]), new Text("C1=" + occurrences));
            } else if (words.length == 3) {
                // For 3-gram
                context.write(new Text(words[2]), new Text("N3=" + occurrences));
                context.write(new Text(words[1] + " " + words[2]), new Text("C2=" + occurrences));
            }
        }
    }

    /**
     * Reducer class:
     * Joins data and calculates the conditional probabilities.
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long N1 = 0, N2 = 0, N3 = 0, C0 = 0, C1 = 0, C2 = 0;

            for (Text value : values) {
                String[] parts = value.toString().split("=");
                String type = parts[0];
                long count = Long.parseLong(parts[1]);

                switch (type) {
                    case "N1": N1 += count; break;
                    case "N2": N2 += count; break;
                    case "N3": N3 += count; break;
                    case "C0": C0 += count; break;
                    case "C1": C1 += count; break;
                    case "C2": C2 += count; break;
                }
            }

            // Compute smoothing factors
            double k2 = Math.log(N2 + 1.0) / (Math.log(N2 + 1.0) + 0.5);
            double k3 = Math.log(N3 + 1.0) / (Math.log(N3 + 1.0) + 2.0);

            // Calculate conditional probability
            double probability = k3 * (N3 / (double) C2) +
                    (1 - k3) * k2 * (N2 / (double) C1) +
                    (1 - k3) * (1 - k2) * (N1 / (double) C0);

            // Emit the result
            context.write(key, new Text(Double.toString(probability)));
        }
    }
}
