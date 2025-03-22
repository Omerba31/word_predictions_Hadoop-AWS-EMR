import org.apache.hadoop.fs.Path;

public class Config {
    public static final String BUCKET_NAME = "dsp-02-bucket";
    public static final String HEB_STOPWORDS_TXT = "heb-stopwords.txt";
    public static final String REGION = "us-east-1";
    public static final String LOGS = "s3://" + BUCKET_NAME + "/logs";

    public static final int NUM_MAP_TASKS = 50;

    public static final Path PATH_1_GRAM = new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data");
    public static final Path PATH_2_GRAM = new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data");
    public static final Path PATH_3_GRAM = new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data");

    public static final Path PATH_C0 = new Path("s3://" + BUCKET_NAME + "/vars/c0.txt");

    public static final Path OUTPUT_STEP_1 = new Path("s3://" + BUCKET_NAME + "/output" + "/Step1");
    public static final Path OUTPUT_STEP_2 = new Path("s3://" + BUCKET_NAME + "/output" + "/Step2");
    public static final Path OUTPUT_STEP_3 = new Path("s3://" + BUCKET_NAME + "/output" + "/Step3");
}

