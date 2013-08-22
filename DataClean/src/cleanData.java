import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import org.jsoup.Jsoup;


public class cleanData {
	public static void main(String[] args){
		
		String movieID = null;
		String userID = null;
		String profileName = null;
		String helpfulness = null;
		String score = null;
		String time = null;
		String summary = null;
		String review = null;
		
		String fieldNames[] = new String[] {"product/productId: ", "review/userId: ", "review/profileName: ",
				"review/helpfulness: ", "review/score: ", "review/time: ", "review/summary: ", "review/text: "};
		
		String outputFile = "/home/sam/output.txt";
		
		try {	
        	FileWriter fileWriter = new FileWriter(outputFile);
        	BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        	
        	FileReader fileReader = new FileReader("/home/sam/movies.txt");
        	BufferedReader bufferedReader = new BufferedReader(fileReader);
        	
        	String lineData; 
        	int count = 1;
    		
        	while ((lineData = bufferedReader.readLine()) != null){
            	
        		int field = 8;
    			
    			for (int i = 0; i<fieldNames.length; i++){
    				if (lineData.contains(fieldNames[i])){
    					field = i;
    					break;
    				}
    			}
    			switch (field){
    			case 0:
    				movieID = lineData.replaceAll(fieldNames[0], "");
    				break;
    			case 1:
    				userID = lineData.replaceAll(fieldNames[1], "");
    				break;
    			case 2:
    				profileName = lineData.replaceAll(fieldNames[2], "");
    				break;
    			case 3:
    				helpfulness = lineData.replaceAll(fieldNames[3], "");
    				break;
    			case 4:
    				score = lineData.replaceAll(fieldNames[4], "");
    				break;
    			case 5:
    				time = lineData.replaceAll(fieldNames[5], "");
    				break;
    			case 6:
    				summary = lineData.replaceAll(fieldNames[6], "");
    				break;
    			case 7:
    				review = lineData.replaceAll(fieldNames[7], "");
    				break;
    			case 8:
    				if (summary != null && review != null){
    					bufferedWriter.write(movieID+"|"+userID+"|"+helpfulness+"|"+score+"|"+time+"|"+Jsoup.parse(summary).text()+"|"+Jsoup.parse(review).text());
    					bufferedWriter.newLine();
    					System.out.print(count);
    					System.out.print('\t');
        				System.out.println(movieID+"|"+userID+"|"+helpfulness+"|"+score+"|"+time+"|"+Jsoup.parse(summary).text()+"|"+Jsoup.parse(review).text());
        				count += 1;
    				}
    				movieID = null;
    				userID = null;
    				profileName = null;
    				helpfulness = null;
    				score = null;
    				time = null;
    				summary = null;
    				review = null;
    			}
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
