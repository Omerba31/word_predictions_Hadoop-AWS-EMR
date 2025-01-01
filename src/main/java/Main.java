import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.HashMap;

public class Main {

    public static AWSCredentialsProvider credentialsProvider;
    public static AmazonS3 s3;

    // Permanent paths for input and output
    private static final String INPUT_1GRAM_PATH = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data";
    private static final String INPUT_2GRAM_PATH = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";
    private static final String INPUT_3GRAM_PATH = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data";
    private static final String OUTPUT_STEP1_PATH = "src/output/step1";
    private static final String OUTPUT_STEP2_PATH = "src/output/step2";
    private static final String STOPWORDS_PATH = "src/resource/heb-stopwords.txt";

    public static void main(String[] args) throws Exception {
        // Initialize AWS if needed
        initializeAWS();

        // Step 1: Process N-Grams
        boolean success = runJob(
                "N-Gram Processing",
                ConditionalProbabilityCalculator.class,
                new String[]{INPUT_1GRAM_PATH, INPUT_2GRAM_PATH, INPUT_3GRAM_PATH},
                OUTPUT_STEP1_PATH,
                STOPWORDS_PATH
        );
        if (!success) {
            System.err.println("N-Gram processing failed.");
            System.exit(1);
        }

        // Step 2: Calculate Conditional Probabilities
        success = runProbabilityCalculationJob(
                "Conditional Probability Calculation",
                OUTPUT_STEP1_PATH,
                OUTPUT_STEP2_PATH
        );
        if (!success) {
            System.err.println("Conditional probability calculation failed.");
            System.exit(1);
        }

        System.out.println("All jobs completed successfully.");
    }

    /**
     * Initialize AWS clients for S3.
     */
    private static void initializeAWS() {
        credentialsProvider = new ProfileCredentialsProvider("default");
        s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
    }

    /**
     * Run a Hadoop job for N-Gram processing.
     */
    private static boolean runJob(String jobName, Class<?> jobClass, String[] inputPaths, String outputPath, String stopWordsPath) throws Exception {
        Configuration conf = new Configuration();
        conf.set("stopwords.path", stopWordsPath);

        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(jobClass);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Add multiple input paths
        for (String inputPath : inputPaths) {
            FileInputFormat.addInputPath(job, new Path(inputPath));
        }

        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true);
    }

    /**
     * Run the final Hadoop job to calculate conditional probabilities.
     */
    private static boolean runProbabilityCalculationJob(String jobName, String inputPath, String outputPath) throws Exception {
        Configuration conf = new Configuration();
        conf.set("input.path", inputPath);

        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(ConditionalProbabilityCalculator.class);

        job.setMapperClass(ConditionalProbabilityCalculator.Map.class);
        job.setReducerClass(ConditionalProbabilityCalculator.Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true);
    }
}
