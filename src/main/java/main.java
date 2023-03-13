
import org.apache.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.*;
import java.util.*;

class  ScalafirstPorject
{
    public static void wordcount(String filename) {
        try {
            SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Words Counter").set("spark.shuffle.service.enabled", "false")
                    .set("spark.dynamicAllocation.enabled", "false");
            JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
            JavaRDD<String> inFile = javaSparkContext.textFile(filename);
            JavaRDD<String> wordsfile = inFile.flatMap(content->Arrays.asList(content.split(" ")).iterator());
            JavaPairRDD countdata = wordsfile.mapToPair(t-> new Tuple2<>(t,1)).reduceByKey((x,y) ->(int) x + (int) y );

            System.out.println("Jafar");
            countdata.saveAsTextFile("C:\\Users\\PC\\OneDrive\\Desktop\\RESULT");

        }catch (Exception e){
            System.out.println("Hamza");
            System.out.println(e.getMessage());
        }
    }
    public static void main(String args[])  //static method
    {
        Scanner sc = new Scanner(System.in);
        System.out.println("Enter File Path :- ");
       String path = sc.nextLine();
       System.out.println(path);
       wordcount(path);
    }
}