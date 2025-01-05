import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Objects;

public class Step2 {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // TODO: implement or remove map method
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            long n1_Occurrences = 0L;
            long c0_Occurrences = 0L;

            String keyStr = key.toString();
            String[] keyWords = keyStr.split(",\\s*"); // Split key into parts

            for (Text value : values) {
                try {
                    long occurrences = Long.parseLong(value.toString());

                    // C0
                    if (keyStr.equals("<**,**,**>")) {
                        // Calculate the total count of all words
                        c0_Occurrences += occurrences;


                    } // N1
                    else {
                        if (Objects.equals(keyWords[0], "**") && Objects.equals(keyWords[1], "**")
                                && !Objects.equals(keyWords[2], "**")) {
                            n1_Occurrences += occurrences;
                        }
                    }
                } catch (NumberFormatException e) {
                    context.getCounter("StepGrams", "InvalidOccurrences").increment(1);
                }
            }

            if (!Objects.equals(keyWords[0], "**") && !Objects.equals(keyWords[1], "**")
                    && !Objects.equals(keyWords[2], "**")) {
                String outputValue = context.getValues().toString() + ", " +
                        "N1 occurrences" + n1_Occurrences + ", " +
                        "C0 occurrences" + c0_Occurrences;

                context.write(key, new Text(outputValue));
            }
        }

        public static class partition extends Partitioner<Text, Text> {
            public int getPartition(Text key, Text value, int numPartitions) {

                String keyStr = key.toString();
                String[] keyWords = keyStr.split(",\\s*"); // Split key into parts

                // C0
                if (keyStr.equals("<**,**,**>")) {
                    // SHOULD: Assign `C0` to all reducers cyclically
                    return (value.hashCode() & Integer.MAX_VALUE) % numPartitions;
                    //TODO: CHECK IF THIS IS CORRECT

                } else {
                    // N1 OR N2, N3, C1, C2
                    // keys <w1,w2,w3> and <**,**,w3> are assigned to the same reducer
                    String thirdWord = keyWords[2].replaceAll("[<>]", "").trim();
                    return (thirdWord.hashCode() & Integer.MAX_VALUE) % numPartitions;

                }
            }
        }
    }
}
