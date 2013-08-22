import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.jsoup.Jsoup;


public class cleanData {
	public static void main(String[] args){
		
		String fieldNames[] = new String[] {"product/productId: ", "review/userId: ", "review/profileName: ",
				"review/helpfulness: ", "review/score: ", "review/time: ", "review/summary: ", "review/text: "};
		
		String outputFile = "/home/sam/output.txt";
		
		try {	
        	FileWriter fileWriter = new FileWriter(outputFile);
        	BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        	
        	FileReader fileReader = new FileReader("/home/sam/movies.txt");
        	BufferedReader bufferedReader = new BufferedReader(fileReader);
        	
        	String lineData; 
        	int count = 0;
    		
        	while ((lineData = bufferedReader.readLine()) != null){
    			bufferedWriter.write(lineData.replace(fieldNames[0], ""));
    			for (int i = 1; i<fieldNames.length; i++){
        			lineData = bufferedReader.readLine();
    				String newValue = lineData.replaceAll(fieldNames[i], "");
    				bufferedWriter.write("|".concat(Jsoup.parse(newValue).text()));
    			}
    			bufferedWriter.newLine();
    			bufferedReader.readLine();
    			
    			count += 1;
    			System.out.println(count);
    		}
		}
		
		catch(FileNotFoundException ex) {
            System.out.println("Unable to open file");				
        }
		
        catch(IOException ex) {
            System.out.println("Error reading file");
        }
	}
}