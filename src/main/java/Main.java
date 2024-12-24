import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
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
        if (args.length < 3) {
            System.err.println("Usage: Main <input path> <output path> <stopwords path>");
            System.exit(-1);
        }

        // AWS Service Initialization
        initializeAWS();

        // Input and Output paths
        String inputPath = args[0];
        String outputPath = args[1];
        String stopWordsPath = args[2];

        // Hadoop Job Configuration
        boolean success1 = runJob("1-gram processing", Step1gram.class, inputPath, outputPath + "/1gram", stopWordsPath);
        if (!success1) {
            System.err.println("1-gram processing failed.");
            System.exit(1);
        }

        boolean success2 = runJob("2-gram processing", Step2gram.class, inputPath, outputPath + "/2gram", stopWordsPath);
        if (!success2) {
            System.err.println("2-gram processing failed.");
            System.exit(1);
        }

        boolean success3 = runJob("3-gram processing", Step3gram.class, inputPath, outputPath + "/3gram", stopWordsPath);
        if (!success3) {
            System.err.println("3-gram processing failed.");
            System.exit(1);
        }

        System.out.println("All jobs completed successfully.");
    }

    private static void initializeAWS() {
        // AWS Credentials
        credentialsProvider = new ProfileCredentialsProvider("default");

        // S3 Client
        s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();

        // EC2 Client
        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();

        // EMR Client
        emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
    }

    private static boolean runJob(String jobName, Class<?> jobClass, String inputPath, String outputPath, String stopWordsPath) throws Exception {
        Configuration conf = new Configuration();
        conf.set("stopwords.path", stopWordsPath);

        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(jobClass);

        // Set Mapper, Reducer, and Partitioner based on the job class
        if (jobClass == Step1gram.class) {
            job.setMapperClass(Step1gram.Map.class);
            job.setReducerClass(Step1gram.Reduce.class);
            job.setPartitionerClass(Step1gram.Partition.class);
        } else if (jobClass == Step2gram.class) {
            job.setMapperClass(Step2gram.Map.class);
            job.setReducerClass(Step2gram.Reduce.class);
            job.setPartitionerClass(Step2gram.Partition.class);
        } else if (jobClass == Step3gram.class) {
            job.setMapperClass(Step3gram.Map.class);
            job.setReducerClass(Step3gram.Reduce.class);
            job.setPartitionerClass(Step3gram.Partition.class);
        } else {
            throw new IllegalArgumentException("Unsupported job class: " + jobClass.getName());
        }

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set Input and Output paths
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // Run the job
        return job.waitForCompletion(true);
    }
}
