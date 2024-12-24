import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class ConditionalProbabilityCalculator {

    public static class Map extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\t");
            if (fields.length < 2) return;

            String threeGram = fields[0]; // w1 w2 w3
            long count = Long.parseLong(fields[1]);

            String[] words = threeGram.split(" ");
            if (words.length == 3) {
                String twoGram = words[0] + " " + words[1]; // Extract w1 w2
                context.write(new Text(twoGram), new Text(words[2] + "\\t" + count)); // Emit w1 w2 as key, w3 and count as value
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private HashMap<String, Long> oneGramCounts = new HashMap<>();
        private HashMap<String, Long> twoGramCounts = new HashMap<>();
        private long totalWords;

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String input1GramPath = conf.get("input1gram.path");
            String input2GramPath = conf.get("input2gram.path");

            oneGramCounts = loadCounts(input1GramPath);
            twoGramCounts = loadCounts(input2GramPath);
            totalWords = oneGramCounts.values().stream().mapToLong(value -> value).sum();
        }

        private HashMap<String, Long> loadCounts(String path) throws IOException {
            HashMap<String, Long> counts = new HashMap<>();
            FileSystem fs = FileSystem.get(new Configuration());
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\\t");
                    if (parts.length == 2) {
                        counts.put(parts[0], Long.parseLong(parts[1]));
                    }
                }
            }
            return counts;
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String twoGram = key.toString();
            long twoGramCount = twoGramCounts.getOrDefault(twoGram, 1L); // N2

            for (Text value : values) {
                String[] fields = value.toString().split("\\t");
                String w3 = fields[0];
                long threeGramCount = Long.parseLong(fields[1]); // N3
                long w3Count = oneGramCounts.getOrDefault(w3, 1L); // N1

                double k2 = (Math.log(twoGramCount + 1) + 1) / (Math.log(twoGramCount + 1) + 2);
                double k3 = (Math.log(threeGramCount + 1) + 1) / (Math.log(threeGramCount + 1) + 2);

                double probability = k3 * ((double) threeGramCount / twoGramCount)
                        + (1 - k3) * k2 * ((double) twoGramCount / totalWords)
                        + (1 - k3) * (1 - k2) * ((double) w3Count / totalWords);

                context.write(new Text(twoGram + " " + w3), new Text(String.valueOf(probability)));
            }
        }
    }
}