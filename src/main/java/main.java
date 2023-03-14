
import Config.SparkConfig;
import org.apache.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.*;

import javax.swing.*;
import java.util.*;

class  ScalafirstPorject {


    public static void main(String args[])  //static method
    {
        SparkConfig sparkConfig = new SparkConfig();

        JFileChooser chooser= new JFileChooser();

        //inputfile
        int choice = chooser.showOpenDialog(null);
        if (choice != JFileChooser.APPROVE_OPTION) return;
        String inputfile = chooser.getSelectedFile().getPath();


        //outputfolder
         choice = chooser.showSaveDialog(null);
         if (choice != JFileChooser.APPROVE_OPTION) return;
         String outputfile = chooser.getSelectedFile().getPath();


         System.out.println(inputfile);
         System.out.println(outputfile);

         sparkConfig.wordCount(inputfile,outputfile);
    }
}