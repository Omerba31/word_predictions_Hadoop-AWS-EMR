import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;

public class Step1 {
    /**
     * Input:
     * Key = lineId (LongWritable)
     * Value = n-gram \t year \t occurrences \t pages \t books (Text)
     * Output:
     * 1) Number of occurrences in the corpus - Key = <w1, w2, w3> Value = occurrences
     * 2) Total number of words in the corpus (N) - Key = ** Value = occurrences
     */
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        HashMap<String, Integer> stopWords = new HashMap<>();

        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            super.setup(context);
            Path stopWordsPath = new Path("src/resource/heb-stopwords.txt");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(stopWordsPath)));
            String[] words;
            List<String> wordList = new ArrayList<>();
            while ((word = br.readLine()) != null) {
                wordList.add(word.trim());
            }
            br.close();
        }

        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] ngram = value.toString().split("\t")[0].split(" ");
            String occurrences = value.toString().split("\t")[2];
            if (ngram.length == 3) {
                String w1 = trigram[0];
                String w2 = trigram[1];
                String w3 = trigram[2];

                if (!stopWords.containsKey(w1) && !stopWords.containsKey(w2) && !stopWords.containsKey(w3)) {
                    context.write(new Text(String.format("<%s>", w1)), new Text(occurrences));
                    context.write(new Text(String.format("<%s>", w2)), new Text(occurrences));
                    context.write(new Text(String.format("<%s>", w3)), new Text(occurrences));
                    context.write(new Text(String.format("<%s, %s>", w1, w2)), new Text(occurrences));
                    context.write(new Text(String.format("<%s, %s>", w2, w3)), new Text(occurrences));
                    context.write(new Text(String.format("<%s, %s, %s>", w1, w2, w3)), new Text(occurrences));
                    context.write(new Text("**"), new Text(occurrences));

                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            if (key.toString().equals("**")) {
                long totalNumberOf3Grams = 0;
                for (Text value : Values) {
                    long occurrencesForOne3gram = Long.parseLong(value.toString());
                    totalNumberOf3Grams += occurrencesForOne3gram;
                }
                context.write(key, new Text(String.format("%d", totalNumberOf3Grams)));
            } else {
                long N;
                for () {

                }
            }
        }

    }
}