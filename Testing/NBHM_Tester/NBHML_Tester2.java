import org.cliffc.high_scale_lib.*;
import java.util.*;
import java.io.*;

public class NBHML_Tester2 {
  public static void main(String args[]) {
    testRemoveIteration();
    testNBHML();
  }

  private static boolean checkViewSizes(int expectedSize, NonBlockingHashMapLong nbhml) {
    boolean result = true;
    Collection v = nbhml.values();
    result &= v.size() == expectedSize;
    Collection k = nbhml.keySet();
    result &= k.size() == expectedSize;
    Collection e = nbhml.entrySet();
    result &= e.size() == expectedSize;
    return result;
  }

  private static int getIterationSize(Iterator it) {
    int result = 0;
    while (it.hasNext()) {
      result++;
      it.next();
    }
    return result;
  }
  
  private static void assertNBHMLCheck(NonBlockingHashMapLong nbhml) { nbhml.check(); }
  private static void assertEquals( int x, int y ) {
    if( x != y ) throw new Error(""+x+" != "+y);
  }
  private static void assertEquals( String x, String y ) {
    if( !x.equals(y) ) throw new Error(""+x+" != "+y);
  }
  private static void assertTrue( String s, boolean P ) {
    if( !P ) throw new Error(s);
  }
  
  public static void testRemoveIteration() {
    NonBlockingHashMapLong<String> nbhml = new NonBlockingHashMapLong<String>();
    
    // Drop things into the map
    long key1 = 0;
    nbhml.put(key1, "v1");
    assertNBHMLCheck(nbhml);
    assertEquals(1, nbhml.size());
    assertTrue("view sizes should be 1 after add", checkViewSizes(1, nbhml));
    assertEquals(1, getIterationSize(nbhml.values().iterator()));
    assertEquals(1, getIterationSize(nbhml.keySet().iterator()));
    assertEquals(1, getIterationSize(nbhml.entrySet().iterator()));
    
    long key2 = 99;
    nbhml.put(key2, "v2");
    assertNBHMLCheck(nbhml);
    assertEquals(2, nbhml.size());
    assertTrue("view sizes should be 2 after add", checkViewSizes(2, nbhml));
    assertEquals(2, getIterationSize(nbhml.values().iterator()));
    assertEquals(2, getIterationSize(nbhml.keySet().iterator()));
    assertEquals(2, getIterationSize(nbhml.entrySet().iterator()));

    assertEquals(nbhml.toString(),"{0=v1, 99=v2}");

    // Serialize it out
    try {
      FileOutputStream fos = new FileOutputStream("NBHML_test.txt");
      ObjectOutputStream out = new ObjectOutputStream(fos);
      out.writeObject(nbhml);
      out.close();
    } catch(IOException ex) {
      ex.printStackTrace();
    }

    // Read it back
    try {
      FileInputStream fis = new FileInputStream("NBHML_test.txt");
      ObjectInputStream in = new ObjectInputStream(fis);
      NonBlockingHashMapLong nbhml2 = (NonBlockingHashMapLong)in.readObject();
      in.close();
      assertEquals(nbhml.toString(),nbhml2.toString());
      nbhml = nbhml2;           // Use the de-serialized version
    } catch(IOException ex) {
      ex.printStackTrace();
    } catch(ClassNotFoundException ex) {
      ex.printStackTrace();
    }
  
    // Now begin removing keys and testing
    nbhml.remove(key2);

    assertNBHMLCheck(nbhml);
    assertEquals(1, nbhml.size());
    assertTrue("view sizes should be 1 after remove again", checkViewSizes(1, nbhml));
    assertEquals(1, getIterationSize(nbhml.values().iterator()));
    assertEquals(1, getIterationSize(nbhml.keySet().iterator()));
    assertEquals(1, getIterationSize(nbhml.entrySet().iterator()));
    
    nbhml.clear();

    assertNBHMLCheck(nbhml);
    assertEquals(0, nbhml.size());
    assertTrue("view sizes should be 0 after clear again", checkViewSizes(0, nbhml));
    assertEquals(0, getIterationSize(nbhml.values().iterator()));
    assertEquals(0, getIterationSize(nbhml.keySet().iterator()));
    assertEquals(0, getIterationSize(nbhml.entrySet().iterator()));
    
  }

  public static final void testNBHML() {
    NonBlockingHashMapLong<String> items = new NonBlockingHashMapLong<String>();
    for (int i=100; i<105; i++) {
      items.put(Long.valueOf(i), String.valueOf(i));
    }
    
    System.out.println("Keys:");
    for (Long id : items.keySet()) {
      System.out.println("\t" + id);
    }
    
    System.out.println("Values:");
    for (String value : items.values()) {
      System.out.println("\t" + value);
    }
    
    System.out.println("Entries:");
    for (Map.Entry<Long, String> entry : items.entrySet()) {
      System.out.println("\t" + entry.getKey() + "=" + entry.getValue());
    }
  }
}
