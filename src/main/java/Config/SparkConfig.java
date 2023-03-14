package Config;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;




@AllArgsConstructor
@Setter
@Getter
public class SparkConfig {

    public  void wordCount(String filename,String Outputfolder) {
        try {
            SparkConf sparkConf = new SparkConf()
                    .setMaster("local")
                    .setAppName("Words Counter").set("spark.shuffle.service.enabled", "false")
                    .set("spark.dynamicAllocation.enabled", "false");

            JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

            JavaRDD<String> inFile = javaSparkContext.textFile(filename);

            JavaRDD<String> wordsfile = inFile.flatMap(content-> Arrays.asList(content.split(" ")).iterator());

            JavaPairRDD countdata = wordsfile.mapToPair(t-> new Tuple2<>(t,1)).reduceByKey((x, y) ->(int) x + (int) y );

            System.out.println("Error1");

            countdata.saveAsTextFile(Outputfolder);

        }catch (Exception e){
            System.out.println("Error2");
            System.out.println(e.getMessage());
        }
    }

    public  void wordcountwithfulldetiles(String filename,String Outputfolder,String address,String appname) {
        try {
            SparkConf sparkConf = new SparkConf().setMaster(address)
                    .setAppName(appname)
                    .set("spark.shuffle.service.enabled", "false")
                    .set("spark.dynamicAllocation.enabled", "false");
            JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
            JavaRDD<String> inFile = javaSparkContext.textFile(filename);
            JavaRDD<String> wordsfile = inFile.flatMap(content-> Arrays.asList(content.split(" ")).iterator());
            JavaPairRDD countdata = wordsfile.mapToPair(t-> new Tuple2<>(t,1)).reduceByKey((x, y) ->(int) x + (int) y );

            System.out.println("Error1");
            countdata.saveAsTextFile(Outputfolder);

        }catch (Exception e){
            System.out.println("Error2");
            System.out.println(e.getMessage());
        }
    }

}
