import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class Main {

    public static AWSCredentialsProvider credentialsProvider;
    public static AmazonS3 s3;
    public static AmazonEC2 ec2;
    public static AmazonElasticMapReduce emr;

    public static void generateStopWord(Mapper<LongWritable, Text, Text, Text>.Context context, HashMap<String, Integer> stopWords) throws IOException {
        Path stopWordsPath = new Path("src/resource/heb-stopwords.txt");
        FileSystem fs = FileSystem.getLocal(context.getConfiguration());
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(stopWordsPath)))) {
            String word;
            while ((word = br.readLine()) != null) {
                stopWords.put(word.trim(), 1);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // Initialize AWS clients
        initializeAWS();

        // Hardcoded paths for input, output, and stopwords file
        String input1GramPath = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data";
        String input2GramPath = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";
        String input3GramPath = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data";
        String outputBasePath = "src/output";
        String stopWordsPath = "src/resource/heb-stopwords.txt";

        boolean success;

        // Step 1: Process 1-grams
        success = runJob("1-gram processing",
                Step1gram.class,
                input1GramPath,
                outputBasePath + "/1gram",
                stopWordsPath);
        if (!success) {
            System.err.println("1-gram processing failed.");
            System.exit(1);
        }

        // Step 2: Process 2-grams
        success = runJob("2-gram processing",
                Step2gram.class,
                input2GramPath,
                outputBasePath + "/2gram",
                stopWordsPath);
        if (!success) {
            System.err.println("2-gram processing failed.");
            System.exit(1);
        }

        // Step 3: Process 3-grams
        success = runJob("3-gram processing",
                Step3gram.class,
                input3GramPath,
                outputBasePath + "/3gram",
                stopWordsPath);
        if (!success) {
            System.err.println("3-gram processing failed.");
            System.exit(1);
        }

        // Step 4: Calculate Conditional Probabilities P(w3 | w1, w2)
        success = runProbabilityCalculationJob(
                "Conditional Probability Calculation",
                outputBasePath + "/1gram",
                outputBasePath + "/2gram",
                outputBasePath + "/3gram",
                outputBasePath + "/conditional_probabilities");
        if (!success) {
            System.err.println("Conditional probability calculation failed.");
            System.exit(1);
        }

        System.out.println("All jobs completed successfully.");
    }

    /**
     * Initialize AWS clients for S3, EC2, and EMR.
     */
    private static void initializeAWS() {
        // AWS Credentials (default profile or environment variables)
        credentialsProvider = new ProfileCredentialsProvider("default");

        // S3 Client: To interact with Amazon S3
        s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();

        // EC2 Client: To manage EC2 instances
        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();

        // EMR Client: To manage EMR clusters
        emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
    }

    /**
     * Run a Hadoop job for 1-gram, 2-gram, or 3-gram processing.
     */
    private static boolean runJob(String jobName, Class<?> jobClass, String inputPath, String outputPath, String stopWordsPath) throws Exception {
        Configuration conf = new Configuration();
        conf.set("stopwords.path", stopWordsPath); // Pass the stopwords path to the configuration

        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(jobClass);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set Input and Output paths
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // Run the job
        return job.waitForCompletion(true);
    }

    /**
     * Run the final Hadoop job to calculate conditional probabilities.
     */
    private static boolean runProbabilityCalculationJob(String jobName, String input1GramPath, String input2GramPath, String input3GramPath, String outputPath) throws Exception {
        Configuration conf = new Configuration();
        conf.set("input1gram.path", input1GramPath);
        conf.set("input2gram.path", input2GramPath);
        conf.set("input3gram.path", input3GramPath);

        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(ConditionalProbabilityCalculator.class);

        job.setMapperClass(ConditionalProbabilityCalculator.Map.class);
        job.setReducerClass(ConditionalProbabilityCalculator.Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set Input and Output paths
        FileInputFormat.addInputPath(job, new Path(input3GramPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // Run the job
        return job.waitForCompletion(true);
    }
}
