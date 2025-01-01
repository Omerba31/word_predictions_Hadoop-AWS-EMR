import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: Main <input_path> <output_path_step1> <output_path_step2>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // Step 1: Process n-grams and compute counts (1-grams, 2-grams, 3-grams)
        Job job1 = Job.getInstance(conf, "Step 1: N-Gram Count");
        job1.setJarByClass(Main.class);
        job1.setMapperClass(StepGrams.Map.class);
        job1.setReducerClass(StepGrams.Reduce.class);
        job1.setPartitionerClass(StepGrams.Partition.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Step 2: Calculate conditional probabilities using the ConditionalProbabilityCalculator
        Job job2 = Job.getInstance(conf, "Step 2: Conditional Probability Calculation");
        job2.setJarByClass(Main.class);
        job2.setMapperClass(ConditionalProbabilityCalculator.Map.class);
        job2.setReducerClass(ConditionalProbabilityCalculator.Reduce.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
