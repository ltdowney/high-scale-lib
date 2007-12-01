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
    assertThat ( _nbhm.putIfAbsent("k1","v1"), nullValue() );
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

  public void testIterationBig() {
    assertThat( _nbhm.size(), is(0) );
    for( int i=0; i<100; i++ ) 
      _nbhm.put("k"+i,"v"+i);
    assertThat( _nbhm.size(), is(100) );

    int sz =0;
    int sum = 0;
    for( String s : _nbhm.keySet() ) {
      sz++;
      assertThat("",s.charAt(0),is('k'));
      int x = Integer.parseInt(s.substring(1));
      sum += x;
      assertTrue(x>=0 && x<=99);
    }
    assertThat("Found 100 ints",sz,is(100));
    assertThat("Found all integers in list",sum,is(100*99/2));

    assertThat( "can remove 3", _nbhm.remove("k3"), is("v3") );
    assertThat( "can remove 4", _nbhm.remove("k4"), is("v4") );
    sz =0;
    sum = 0;
    for( String s : _nbhm.keySet() ) {
      sz++;
      assertThat("",s.charAt(0),is('k'));
      int x = Integer.parseInt(s.substring(1));
      sum += x;
      assertTrue(x>=0 && x<=99);
      String v = _nbhm.get(s);
      assertThat("",v.charAt(0),is('v'));
      assertThat("",s.substring(1),is(v.substring(1)));
    }
    assertThat("Found 98 ints",sz,is(98));
    assertThat("Found all integers in list",sum,is(100*99/2 - (3+4)));
  }

  // Do some simple concurrent testing
  public void testConcurrentSimple() throws InterruptedException {
    final NonBlockingHashMap<String,String> nbhm = new NonBlockingHashMap<String,String>();

    // In 2 threads, add & remove even & odd elements concurrently
    Thread t1 = new Thread() { public void run() { work_helper(nbhm,"T1",1); } };
    t1.start();
    work_helper(nbhm,"T0",0);
    t1.join();
    
    // In the end, all members should be removed
    StringBuffer buf = new StringBuffer();
    buf.append("Should be emptyset but has these elements: {");
    boolean found = false;
    for( String x : nbhm.keySet() ) {
      buf.append(" ").append(x);
      found = true;
    }
    if( found ) System.out.println(buf+" }");
    assertThat( "concurrent size=0", nbhm.size(), is(0) );
    for( String x : nbhm.keySet() ) {
      assertTrue("No elements so never get here",false);
    }
  }

  void work_helper(NonBlockingHashMap<String,String> nbhm, String thrd, int d) {
    final int ITERS = 20000;
    for( int j=0; j<10; j++ ) {
      long start = System.nanoTime();
      for( int i=d; i<ITERS; i+=2 )
        assertThat( "this key not in there, so putIfAbsent must work", 
                    nbhm.putIfAbsent("k"+i,thrd), is((String)null) );
      for( int i=d; i<ITERS; i+=2 )
        assertTrue( nbhm.remove("k"+i,thrd) );
      double delta_nanos = System.nanoTime()-start;
      double delta_secs = delta_nanos/1000000000.0;
      double ops = ITERS*2;
      //System.out.println("Thrd"+thrd+" "+(ops/delta_secs)+" ops/sec size="+nbhm.size());
    }
  }

}
