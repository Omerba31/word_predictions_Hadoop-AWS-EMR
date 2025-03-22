import com.amazonaws.services.s3.model.ObjectListing;
import org.apache.hadoop.fs.Path;

public class Config {
    public static final String BUCKET_NAME = "dsp-02-bucket";
    public static final String REGION = "us-east-1";
    public static final String LOGS = "s3://" + BUCKET_NAME + "/logs";

    public static final Path PATH_1_GRAM = new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data");
    public static final Path PATH_2_GRAM = new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data");
    public static final Path PATH_3_GRAM = new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data");

    public static final Path OUTPUT_STEP_1 = new Path("s3://" + BUCKET_NAME + "/output" + "/Step1");
    public static final Path OUTPUT_STEP_2 = new Path("s3://" + BUCKET_NAME + "/output" + "/Step2");
    public static final Path OUTPUT_STEP_3 = new Path("s3://" + BUCKET_NAME + "/output" + "/Step3");

    public static String[] stopWordsArr = {
            "״", "׳", "של", "רב", "פי", "עם", "עליו", "עליהם", "על", "עד", "מן", "מכל", "מי",
            "מהם", "מה", "מ", "למה", "לכל", "לי", "לו", "להיות", "לה", "לא", "כן", "כמה", "כלי",
            "כל", "כי", "יש", "ימים", "יותר", "יד", "י", "זה", "ז", "ועל", "ומי", "ולא", "וכן",
            "וכל", "והיא", "והוא", "ואם", "ו", "הרבה", "הנה", "היו", "היה", "היא", "הזה", "הוא",
            "דבר", "ד", "ג", "בני", "בכל", "בו", "בה", "בא", "את", "אשר", "אם", "אלה", "אל",
            "אך", "איש", "אין", "אחת", "אחר", "אחד", "אז", "אותו", "־", "^", "?", ";", ":", "1",
            ".", "-", "*", "\"", "!", "שלשה", "בעל", "פני", ")", "גדול", "שם", "עלי", "עולם",
            "מקום", "לעולם", "לנו", "להם", "ישראל", "יודע", "זאת", "השמים", "הזאת", "הדברים",
            "הדבר", "הבית", "האמת", "דברי", "במקום", "בהם", "אמרו", "אינם", "אחרי", "אותם",
            "אדם", "(", "חלק", "שני", "שכל", "שאר", "ש", "ר", "פעמים", "נעשה", "ן", "ממנו",
            "מלא", "מזה", "ם", "לפי", "ל", "כמו", "כבר", "כ", "זו", "ומה", "ולכל", "ובין",
            "ואין", "הן", "היתה", "הא", "ה", "בל", "בין", "בזה", "ב", "אף", "אי", "אותה",
            "או", "אבל", "א"
    };


}

