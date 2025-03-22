import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
//import com.amazonaws.services.elasticmapreduce.model.InstanceType;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static AWSCredentialsProvider credentialsProvider;
    public static AmazonS3 S3;
    public static AmazonEC2 ec2;
    public static AmazonElasticMapReduce emr;
    public static final int numberOfInstances = 7;

    public static void main(String[] args) {
        credentialsProvider = new ProfileCredentialsProvider();
        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(Config.REGION)
                .build();
        S3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(Config.REGION)
                .build();
        emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(Config.REGION)
                .build();

        HadoopJarStepConfig Step1_jar = new HadoopJarStepConfig()
                .withJar("s3://" + Config.BUCKET_NAME + "/jars/Step1.jar")
                .withMainClass("Step1");

        StepConfig Step1_Config = new StepConfig()
                .withName("Step1")
                .withHadoopJarStep(Step1_jar)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig Step2_jar = new HadoopJarStepConfig()
                .withJar("s3://" + Config.BUCKET_NAME + "/jars/Step2.jar")
                .withMainClass("Step2");

        StepConfig Step2_Config = new StepConfig()
                .withName("Step2")
                .withHadoopJarStep(Step2_jar)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig Step3_jar = new HadoopJarStepConfig()
                .withJar("s3://" + Config.BUCKET_NAME + "/jars/Step3.jar")
                .withMainClass("Step3");

        StepConfig Step3_Config = new StepConfig()
                .withName("Step3")
                .withHadoopJarStep(Step3_jar)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(numberOfInstances)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("3.4.1")
                .withEc2KeyName("vockey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Map reduce project")
                .withInstances(instances)
                .withSteps(Step1_Config, Step2_Config, Step3_Config)
                .withLogUri(Config.LOGS)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0");

        cleanS3Directory("logs/");
        cleanS3Directory("output/");

        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        System.out.println("Job flow started with ID: " + runJobFlowResult.getJobFlowId());
    }

    public static void cleanS3Directory(String dirName) {
        if (dirName == null || dirName.trim().isEmpty()) {
            throw new IllegalArgumentException("Directory name cannot be null or empty");
        }

        System.out.println("Cleaning S3 directory: " + dirName);

        ObjectListing objectListing = S3.listObjects(Config.BUCKET_NAME, dirName);
        while (true) {
            List<DeleteObjectsRequest.KeyVersion> keysToDelete = new ArrayList<>();
            for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
                keysToDelete.add(new DeleteObjectsRequest.KeyVersion(summary.getKey()));
            }

            if (!keysToDelete.isEmpty()) {
                DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(Config.BUCKET_NAME)
                        .withKeys(keysToDelete);
                S3.deleteObjects(deleteRequest);
            }

            if (objectListing.isTruncated()) {
                objectListing = S3.listNextBatchOfObjects(objectListing);
            } else {
                break;
            }
        }
    }

}
