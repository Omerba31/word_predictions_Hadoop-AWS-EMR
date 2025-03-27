README

Names and IDs:
- Omer Ben Arie (ID: 211602842)
- Shahaf Kita

---

📌 Project Overview:
This project implements a 3-step MapReduce pipeline on AWS EMR to compute interpolated probabilities for Hebrew trigrams (3-grams), using Google's NGram dataset. It estimates the probability of a word given its previous two words via a backoff model with interpolation weights.

---

🚀 How to Run the Project:

1. **Set Up AWS Credentials:**
   Make sure your AWS credentials are configured using the AWS CLI or located in `~/.aws/credentials`.

2. **Ensure EC2 Key Pair Exists:**
   A key pair named `vockey` must exist in your AWS region.

3. **Upload JAR Files:**
   Upload the following JARs to your S3 bucket:
   s3://<YOUR-BUCKET>/jars/

4. **Compile and Run Main Class:**
   mvn clean package
   mvn exec:java -Dexec.mainClass="Main"

✅ This will:
- Launch an EMR cluster with 7 instances
- Execute all 3 MapReduce steps
- Write logs to: s3://<YOUR-BUCKET>/logs/
- Write outputs to: s3://<YOUR-BUCKET>/output/

---

📁 Project Files Included in ZIP:

1. **Source Code:**
   - Config.java
   - Main.java
   - Step1.java
   - Step2.java
   - Step3.java

2. **Reports:**
   - KV
   - Interesting word pairs.docx
   - Scalability
   - Final Output

3. **Output Directories:**
   - Step1/
   - Step2/
   - Step3/

---

📝 Notes:
- EMR Version: emr-5.11.0
- Hadoop Version: 3.4.1
- Input Data:
  s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/

- Output:
  - Trigram probabilities sorted by context and descending probability
  - C0.txt: Total valid 1-gram count stored at:
    s3://<YOUR-BUCKET>/vars/C0.txt

---