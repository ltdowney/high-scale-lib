/*
 * Written by Cliff Click and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

import org.cliffc.high_scale_lib.*;
import java.util.*;
import java.io.*;
import junit.framework.TestCase;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

// Test NonBlockingHashMap via JUnit
public class NBHM_Tester2 extends TestCase {
  public static void main(String args[]) {
    org.junit.runner.JUnitCore.main("NBHM_Tester2");
  }

  private NonBlockingHashMap<String,String> _nbhm;
  protected void setUp   () { _nbhm = new NonBlockingHashMap<String,String>(); }
  protected void tearDown() { _nbhm = null; }

  // Test some basic stuff; add a few keys, remove a few keys
  public void testBasic() {
    assertTrue ( _nbhm.isEmpty() );
    assertThat ( _nbhm.put("k1","v1"), nullValue() );
    checkSizes (1);
    assertThat ( _nbhm.put("k2","v2"), nullValue() );
    checkSizes (2);
    assertThat ( _nbhm.put("k1","v1a"), is("v1") );
    assertThat ( _nbhm.put("k2","v2a"), is("v2") );
    checkSizes (2);
    assertThat ( _nbhm.remove("k1"), is("v1a") );
    checkSizes (1);
    assertThat ( _nbhm.remove("k1"), nullValue() );
    assertThat ( _nbhm.remove("k2"), is("v2a") );
    checkSizes (0);
    assertThat ( _nbhm.remove("k2"), nullValue() );
    assertThat ( _nbhm.remove("k3"), nullValue() );
    assertTrue ( _nbhm.isEmpty() );
  }

  // Check all iterators for correct size counts
  private void checkSizes(int expectedSize) {
    assertEquals( "size()", _nbhm.size(), expectedSize );
    Collection vals = _nbhm.values();
    checkSizes("values()",vals.size(),vals.iterator(),expectedSize);
    Set keys = _nbhm.keySet();
    checkSizes("keySet()",keys.size(),keys.iterator(),expectedSize);
    Set ents = _nbhm.entrySet();
    checkSizes("entrySet()",ents.size(),ents.iterator(),expectedSize);
  }

  // Check that the iterator iterates the correct number of times
  private void checkSizes(String msg, int sz, Iterator it, int expectedSize) {
    assertEquals( msg, expectedSize, sz );
    int result = 0;
    while (it.hasNext()) {
      result++;
      it.next();
    }
    assertEquals( msg, expectedSize, result );
  }
  

  public void testIteration() {
    assertTrue ( _nbhm.isEmpty() );
    assertThat ( _nbhm.put("k1","v1"), nullValue() );
    assertThat ( _nbhm.put("k2","v2"), nullValue() );

    String str1 = "";
    for( Iterator<Map.Entry<String,String>> i = _nbhm.entrySet().iterator(); i.hasNext(); ) {
      Map.Entry<String,String> e = i.next();
      str1 += e.getKey();
    }
    assertThat("found all entries",str1,anyOf(is("k1k2"),is("k2k1")));

    String str2 = "";
    for( Iterator<String> i = _nbhm.keySet().iterator(); i.hasNext(); ) {
      String key = i.next();
      str2 += key;
    }
    assertThat("found all keys",str2,anyOf(is("k1k2"),is("k2k1")));

    String str3 = "";
    for( Iterator<String> i = _nbhm.values().iterator(); i.hasNext(); ) {
      String val = i.next();
      str3 += val;
    }
    assertThat("found all vals",str3,anyOf(is("v1v2"),is("v2v1")));

    assertThat("toString works",_nbhm.toString(), anyOf(is("{k1=v1, k2=v2}"),is("{k2=v2, k1=v1}")));
  }

  public void testSerial() {
    assertTrue ( _nbhm.isEmpty() );
    assertThat ( _nbhm.put("k1","v1"), nullValue() );
    assertThat ( _nbhm.put("k2","v2"), nullValue() );

    // Serialize it out
    try {
      FileOutputStream fos = new FileOutputStream("NBHM_test.txt");
      ObjectOutputStream out = new ObjectOutputStream(fos);
      out.writeObject(_nbhm);
      out.close();
    } catch(IOException ex) {
      ex.printStackTrace();
    }

    // Read it back
    try {
      FileInputStream fis = new FileInputStream("NBHM_test.txt");
      ObjectInputStream in = new ObjectInputStream(fis);
      NonBlockingHashMap nbhm = (NonBlockingHashMap)in.readObject();
      in.close();
      assertEquals(_nbhm.toString(),nbhm.toString());
    } catch(IOException ex) {
      ex.printStackTrace();
    } catch(ClassNotFoundException ex) {
      ex.printStackTrace();
    }

  }
}
