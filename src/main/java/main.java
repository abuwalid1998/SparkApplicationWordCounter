import Config.SparkConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import javax.swing.*;
import java.util.ArrayList;
import java.util.List;

class  ScalafirstPorject {


    public static void main(String args[])  //static method
    {

        //Start Word Counter for this

       SparkConfig sparkConfig = new SparkConfig();
//
//        JFileChooser chooser= new JFileChooser();
//
//        //inputfile
//        int choice = chooser.showOpenDialog(null);
//        if (choice != JFileChooser.APPROVE_OPTION) return;
//        String inputfile = chooser.getSelectedFile().getPath();
//
//
//        //outputfolder
//         choice = chooser.showSaveDialog(null);
//         if (choice != JFileChooser.APPROVE_OPTION) return;
//         String outputfile = chooser.getSelectedFile().getPath();
//
//
//         System.out.println(inputfile);
//         System.out.println(outputfile);
//
//         sparkConfig.wordCount(inputfile,outputfile);


        //Start Pi Calculator
       String slices =  JOptionPane.showInputDialog("Enter Number of slices less than 1000");
       int num = Integer.parseInt(slices);
       if (num > 1000){
           num = 999;
       }
       sparkConfig.CalculatePi(num);
    }


}