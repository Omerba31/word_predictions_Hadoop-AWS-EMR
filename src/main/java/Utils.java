import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.IOException;

public class Utils {

    //     The real path for the 1-gram, 2-gram, and 3-gram data files in S3
    public static final String INPUT_PATH_1GRAM = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data";
    public static final String INPUT_PATH_2GRAM = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";
    public static final String INPUT_PATH_3GRAM = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data";
    public static final String OUTPUT_STEP1_PATH = "s3://dsp-02-bucket/output_step_1/";
    public static final String OUTPUT_STEP2_PATH = "s3://dsp-02-bucket/output_step_2/";
    public static final String OUTPUT_STEP3_PATH = "s3://dsp-02-bucket/output_step_3/";
    public static final Logger LOG = Logger.getLogger(Step1.Reduce.class);

    /**
     * Deletes a directory in S3 if it already exists.
     *
     * @param path The S3 path to delete.
     * @param conf The Hadoop Configuration object.
     * @throws IOException if an error occurs during deletion.
     */
    public static void deleteIfExists(String path, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path s3Path = new Path(path);
        if (fs.exists(s3Path)) {
            System.out.println("Deleting existing path: " + path);
            fs.delete(s3Path, true); // Recursive delete
        }
    }
}
