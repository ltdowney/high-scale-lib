import org.cliffc.high_scale_lib.*;
import java.util.*;
import java.io.*;

public class NBHM_Tester2 {
  public static void main(String args[]) {
    new NBHM_Tester2().testRemoveIteration();
  }

  private boolean checkViewSizes(int expectedSize, NonBlockingHashMap nbhm) {
    boolean result = true;
    Collection v = nbhm.values();
    result &= v.size() == expectedSize;
    Collection k = nbhm.keySet();
    result &= k.size() == expectedSize;
    Collection e = nbhm.entrySet();
    result &= e.size() == expectedSize;
    return result;
  }

  private int getIterationSize(Iterator it) {
    int result = 0;
    while (it.hasNext()) {
      result++;
      it.next();
    }
    return result;
  }
  
  private void assertNBHMCheck(NonBlockingHashMap nbhm) { nbhm.check(); }
  private void assertEquals( int x, int y ) {
    if( x != y ) throw new Error(""+x+" != "+y);
  }
  private void assertEquals( String x, String y ) {
    if( !x.equals(y) ) throw new Error(""+x+" != "+y);
  }
  private void assertTrue( String s, boolean P ) {
    if( !P ) throw new Error(s);
  }
  
  public void testRemoveIteration() {
    NonBlockingHashMap<String,String> nbhm = new NonBlockingHashMap<String,String>();
    
    // Drop things into the map
    String key1 = "k1";
    nbhm.put(key1, "v1");
    assertNBHMCheck(nbhm);
    assertEquals(1, nbhm.size());
    assertTrue("view sizes should be 1 after add", checkViewSizes(1, nbhm));
    assertEquals(1, getIterationSize(nbhm.values().iterator()));
    assertEquals(1, getIterationSize(nbhm.keySet().iterator()));
    assertEquals(1, getIterationSize(nbhm.entrySet().iterator()));
    
    String key2 = "k2";
    nbhm.put(key2, "v2");
    assertNBHMCheck(nbhm);
    assertEquals(2, nbhm.size());
    assertTrue("view sizes should be 2 after add", checkViewSizes(2, nbhm));
    assertEquals(2, getIterationSize(nbhm.values().iterator()));
    assertEquals(2, getIterationSize(nbhm.keySet().iterator()));
    assertEquals(2, getIterationSize(nbhm.entrySet().iterator()));

    assertEquals(nbhm.toString(),"{k1=v1, k2=v2}");

    // Serialize it out
    try {
      FileOutputStream fos = new FileOutputStream("NBHM_test.txt");
      ObjectOutputStream out = new ObjectOutputStream(fos);
      out.writeObject(nbhm);
      out.close();
    } catch(IOException ex) {
      ex.printStackTrace();
    }

    // Read it back
    try {
      FileInputStream fis = new FileInputStream("NBHM_test.txt");
      ObjectInputStream in = new ObjectInputStream(fis);
      NonBlockingHashMap nbhm2 = (NonBlockingHashMap)in.readObject();
      in.close();
      assertEquals(nbhm.toString(),nbhm2.toString());
      nbhm = nbhm2;           // Use the de-serialized version
    } catch(IOException ex) {
      ex.printStackTrace();
    } catch(ClassNotFoundException ex) {
      ex.printStackTrace();
    }
  
    // Now begin removing keys and testing
    nbhm.remove(key2);

    assertNBHMCheck(nbhm);
    assertEquals(1, nbhm.size());
    assertTrue("view sizes should be 1 after remove again", checkViewSizes(1, nbhm));
    assertEquals(1, getIterationSize(nbhm.values().iterator()));
    assertEquals(1, getIterationSize(nbhm.keySet().iterator()));
    assertEquals(1, getIterationSize(nbhm.entrySet().iterator()));
    
    nbhm.clear();

    assertNBHMCheck(nbhm);
    assertEquals(0, nbhm.size());
    assertTrue("view sizes should be 0 after clear again", checkViewSizes(0, nbhm));
    assertEquals(0, getIterationSize(nbhm.values().iterator()));
    assertEquals(0, getIterationSize(nbhm.keySet().iterator()));
    assertEquals(0, getIterationSize(nbhm.entrySet().iterator()));
    
  }
}
