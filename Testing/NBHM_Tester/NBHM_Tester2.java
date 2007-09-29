import org.cliffc.high_scale_lib.*;
import java.util.*;

public class NBHM_Tester2 {
  public static void main(String args[]) {
    new NBHM_Tester2().testRemoveIteration();
  }

  private boolean checkViewSizes(int expectedSize, Map map) {
    boolean result = true;
    Collection v = map.values();
    result &= v.size() == expectedSize;
    Collection k = map.keySet();
    result &= k.size() == expectedSize;
    Collection e = map.entrySet();
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
  private void assertTrue( String s, boolean P ) {
    if( !P ) throw new Error(s);
  }
  
  public void testRemoveIteration() {
    NonBlockingHashMap<String, String> nbhm = new NonBlockingHashMap<String, String>();
    
    // Drop things into the map
    final String key = "s1";
    nbhm.put(key, "v1");
    assertNBHMCheck(nbhm);
    assertEquals(1, nbhm.size());
    assertTrue("view sizes should be 1 after add", checkViewSizes(1, nbhm));
    assertEquals(1, getIterationSize(nbhm.values().iterator()));
    assertEquals(1, getIterationSize(nbhm.keySet().iterator()));
    assertEquals(1, getIterationSize(nbhm.entrySet().iterator()));
    
    nbhm.remove(key);
    assertNBHMCheck(nbhm);
    assertEquals(0, nbhm.size());
    assertTrue("view sizes should be 0 after remove again", checkViewSizes(0, nbhm));
    assertEquals(0, getIterationSize(nbhm.values().iterator()));
    assertEquals(0, getIterationSize(nbhm.keySet().iterator()));
    assertEquals(0, getIterationSize(nbhm.entrySet().iterator()));
  }
}
