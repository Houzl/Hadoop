/**
 * 
 */
package h.GeneticAlgorithm;

import java.util.Random;

/**
 * @author Zhilong Hou, 001707727
 *
 */
public class ChromosomesStatic {
  private static String[] geneTableBinaryString = {"0000","0001","0010","0011","0100","0101","0110","0111","1000","1001","1010","1011","1100","1101"};
 
  public static String randomGenerate(int length, Random rand) {
    String chromosomes = "";
    for (int i = 0; i < length; i++ ){
      if (i%2 == 0) chromosomes += geneTableBinaryString[rand.nextInt(10)];
      else chromosomes += geneTableBinaryString[rand.nextInt(4) + 10];
    }
    return chromosomes;
  }  
  
  public static double fitnessScore(String chromosomes, double target){
    int num;
    double tmp = 0;
    String operator = "";
    for (int i = 0; i < chromosomes.length() ; i += 4 ){
      if (i%8 == 0){
        num = Integer.parseInt(chromosomes.substring(i, i+4), 2);
        switch (operator){
          case "": 
            tmp = num;
            break;
          case "1010": 
            tmp += num;
            break;
          case "1011": 
            tmp -= num;
            break; 
          case "1100": 
            tmp *= num;
            break; 
          case "1101":
            if (num != 0) tmp /= num;
            break;
        }
      }
      else operator = chromosomes.substring(i, i+4);
    }
    return target == tmp ? 1.0 : 1.0/(target - tmp);
  }

  public static double score(String chromosomes){
    int num;
    double tmp = 0;
    String operator = "";
    for (int i = 0; i < chromosomes.length() ; i += 4 ){
      if (i%8 == 0){
        num = Integer.parseInt(chromosomes.substring(i, i+4), 2);
        switch (operator){
          case "": 
            tmp = num;
            break;
          case "1010": 
            tmp += num;
            break;
          case "1011": 
            tmp -= num;
            break; 
          case "1100": 
            tmp *= num;
            break; 
          case "1101":
            if (num != 0) tmp /= num;
            break;
        }
      }
      else operator = chromosomes.substring(i, i+4);
    }
    return tmp;
  }
  
  public static String decode(String chromosomes){
    String operator = "";
    String reslut = "";
    for (int i = 0; i < chromosomes.length() ; i += 4 ){
      if (i%8 == 0){
        reslut  += Integer.parseInt(chromosomes.substring(i, i+4), 2);
      }
      else {
        operator = chromosomes.substring(i, i+4);
        switch (operator){
          case "1010": 
            reslut += "+";
            break;
          case "1011": 
            reslut += "-";
            break; 
          case "1100": 
            reslut += "*";
            break; 
          case "1101":
            reslut += "/";
            break;
        }
      }
    }
    return reslut;
  }  
  
  public static String[] crossOver(String a, String b, double crossoverRate, Random rand){
    if (a.length() != b.length()){
      System.out.println("The length of chromosomes is not equal");
      return null;
    }
    String[] result = {a,b};
    // Should we cross over?
    if (rand.nextDouble() <= crossoverRate) {
      // Generate a random position
      // The position is gene based, each gene has 4 bits.
      int pos = rand.nextInt(a.length()/4) * 4;
      
      String a1 = a.substring(0, pos);
      String a2 = a.substring(pos);
      
      String b1 = b.substring(0, pos);
      String b2 = b.substring(pos);
      
      result[0] = a1 + b2;
      result[1] = b1 + a2;
    }
    return result;
  }

  public static String mutate(String a, double mutateRate, Random rand) {
    String reslut = ""; 
    for (int i=0 ; i < a.length(); i++) {
      reslut += rand.nextDouble() <= mutateRate ? (a.charAt(i)=='0' ? '1' : '0') : a.charAt(i);
    }
    for (int i = 0; i < reslut.length()/4 ; i++ ){
      int num = Integer.parseInt(reslut.substring(i*4, i*4+4), 2);
      if (i%2 == 0){
        if (num > 9){
          reslut = a;
          break;
        }
      }
      else{
        if (num < 10 || num > 13){
          reslut = a;
          break;
        }
      }
    }
    return reslut;
  }

}
