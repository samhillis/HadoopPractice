import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class testSplit {
	public static void main(String[] args){

		try {
        	FileReader fileReader = new FileReader("/home/sam/short.txt");
        	BufferedReader bufferedReader = new BufferedReader(fileReader);
        	
        	String lineData;
        	ArrayList<String> movies = new ArrayList<String>();
        	
        	Integer count = 0;
        	
        	while ((lineData = bufferedReader.readLine()) != null){
	    		String[] fields = lineData.split("\\|");
        		if (! movies.contains(fields[0])){
        			count += (Integer) 1;
        			movies.add(fields[0]);
        		}
        	}
        	System.out.println(count);
		}
		
		catch(FileNotFoundException ex) {
            System.out.println("Unable to open file");				
        }
		
        catch(IOException ex) {
            System.out.println("Error reading file");
        }
	}
}

