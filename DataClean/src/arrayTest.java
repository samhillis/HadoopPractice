
public class arrayTest {
	public static void main(String[] args){
		String test = "This	is	a	test";
		String[] testArray = test.split("\t");
		for (int i=0; i<testArray.length; i++){
			System.out.println(testArray[i]);
		}
	}
}
