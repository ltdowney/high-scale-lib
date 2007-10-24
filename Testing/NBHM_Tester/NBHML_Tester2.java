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

// Test NonBlockingHashMapLong via JUnit
public class NBHML_Tester2 extends TestCase {

  private NonBlockingHashMapLong<String> _nbhml;
  protected void setUp   () { _nbhml = new NonBlockingHashMapLong<String>(); }
  protected void tearDown() { _nbhml = null; }

  // Test some basic stuff; add a few keys, remove a few keys
  public void testBasic() {
    assertTrue ( _nbhml.isEmpty() );
    assertThat ( _nbhml.put(1,"v1"), nullValue() );
    checkSizes (1);
    assertThat ( _nbhml.put(2,"v2"), nullValue() );
    checkSizes (2);
    assertThat ( _nbhml.put(1,"v1a"), is("v1") );
    assertThat ( _nbhml.put(2,"v2a"), is("v2") );
    checkSizes (2);
    assertThat ( _nbhml.remove(1), is("v1a") );
    checkSizes (1);
    assertThat ( _nbhml.remove(1), nullValue() );
    assertThat ( _nbhml.remove(2), is("v2a") );
    checkSizes (0);
    assertThat ( _nbhml.remove(2), nullValue() );
    assertThat ( _nbhml.remove("k3"), nullValue() );
    assertTrue ( _nbhml.isEmpty() );
  }

  // Check all iterators for correct size counts
  private void checkSizes(int expectedSize) {
    assertEquals( "size()", _nbhml.size(), expectedSize );
    Collection vals = _nbhml.values();
    checkSizes("values()",vals.size(),vals.iterator(),expectedSize);
    Set keys = _nbhml.keySet();
    checkSizes("keySet()",keys.size(),keys.iterator(),expectedSize);
    Set ents = _nbhml.entrySet();
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
    assertTrue ( _nbhml.isEmpty() );
    assertThat ( _nbhml.put(1,"v1"), nullValue() );
    assertThat ( _nbhml.put(2,"v2"), nullValue() );

    String str1 = "";
    for( Iterator<Map.Entry<Long,String>> i = _nbhml.entrySet().iterator(); i.hasNext(); ) {
      Map.Entry<Long,String> e = i.next();
      str1 += e.getKey();
    }
    assertThat("found all entries",str1,anyOf(is("12"),is("21")));

    String str2 = "";
    for( Iterator<Long> i = _nbhml.keySet().iterator(); i.hasNext(); ) {
      Long key = i.next();
      str2 += key;
    }
    assertThat("found all keys",str2,anyOf(is("12"),is("21")));

    String str3 = "";
    for( Iterator<String> i = _nbhml.values().iterator(); i.hasNext(); ) {
      String val = i.next();
      str3 += val;
    }
    assertThat("found all vals",str3,anyOf(is("v1v2"),is("v2v1")));

    assertThat("toString works",_nbhml.toString(), anyOf(is("{1=v1, 2=v2}"),is("{2=v2, 1=v1}")));
  }

  public void testSerial() {
    assertTrue ( _nbhml.isEmpty() );
    assertThat ( _nbhml.put(1,"v1"), nullValue() );
    assertThat ( _nbhml.put(2,"v2"), nullValue() );

    // Serialize it out
    try {
      FileOutputStream fos = new FileOutputStream("NBHM_test.txt");
      ObjectOutputStream out = new ObjectOutputStream(fos);
      out.writeObject(_nbhml);
      out.close();
    } catch(IOException ex) {
      ex.printStackTrace();
    }

    // Read it back
    try {
      FileInputStream fis = new FileInputStream("NBHM_test.txt");
      ObjectInputStream in = new ObjectInputStream(fis);
      NonBlockingHashMapLong nbhml = (NonBlockingHashMapLong)in.readObject();
      in.close();
      assertEquals(_nbhml.toString(),nbhml.toString());
    } catch(IOException ex) {
      ex.printStackTrace();
    } catch(ClassNotFoundException ex) {
      ex.printStackTrace();
    }

  }
}
