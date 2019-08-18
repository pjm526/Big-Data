import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class GrepSpark {

    public static void main(String[] args) {

        String master = "local[3]";
        String input = "input.txt";
        String output = "output/";

        System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");

        JavaSparkContext sc = null;
        String words[] = new String[] {"but", "and"};

        try {
            SparkConf sparkConf = new SparkConf().setAppName("Grep").setMaster(master).set("spark.hadoop.validateOutputSpecs", "false");
            sc = new JavaSparkContext(sparkConf);

            JavaRDD<String> rdd = sc.textFile(input);
            for (int i = 0; i < words.length; i++) {
                final String pattern = words[i];
                JavaRDD<String> result = rdd.filter(
                        new Function<String, Boolean>() {
                            private static final long serialVersionUID = 1L;
                            Pattern p = Pattern.compile(pattern);

                            public Boolean call(String value) throws Exception {
                                if (value == null || value.length() == 0) {
                                    return false;
                                }
                                final Matcher m = p.matcher(value);
                                if (m.find()) {
                                    return true;
                                }
                                return false;
                            }
                        });
                result.saveAsTextFile(output + pattern);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (sc != null)
                sc.close();
        }
    }

}