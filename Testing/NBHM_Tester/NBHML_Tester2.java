import org.cliffc.high_scale_lib.*;
import java.util.*;

public class NBHML_Tester2 {
  public static void main(String args[]) {
    new NBHML_Tester2().testRemoveIteration();
  }

  private boolean checkViewSizes(int expectedSize, NonBlockingHashMapLong nbhml) {
    boolean result = true;
    Collection v = nbhml.values();
    result &= v.size() == expectedSize;
    Collection k = nbhml.keySet();
    result &= k.size() == expectedSize;
    Collection e = nbhml.entrySet();
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
  
  private void assertNBHMLCheck(NonBlockingHashMapLong nbhml) { nbhml.check(); }
  private void assertEquals( int x, int y ) {
    if( x != y ) throw new Error(""+x+" != "+y);
  }
  private void assertTrue( String s, boolean P ) {
    if( !P ) throw new Error(s);
  }
  
  public void testRemoveIteration() {
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
}
