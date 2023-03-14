package Config;

import lombok.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


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

    public void CalculatePi(int slices){

        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("PI Calculator").set("spark.shuffle.service.enabled", "false")
                .set("spark.dynamicAllocation.enabled", "false");
        final JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        //final int slices = 1000;
        final int n = 100000 * slices;
        final List<Integer> l = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            l.add(i);
        }
        final JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);



        final int count = dataSet.map(integer -> {
            double x = Math.random() * 2 - 1;
            double y = Math.random() * 2 - 1;
            return (x * x + y * y < 1) ? 1 : 0;
        }).reduce((a, b) -> a + b);

      String pi =""+4.0 * count / n;

        try {
            String content = pi;
            String path = "Output/PI.text";
            Files.write( Paths.get(path), content.getBytes());

        }catch (Exception e){
            System.out.println("Pi is roughly " + 4.0 * count / n);
        }



    }

}
