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

// Test NonBlockingSetInt via JUnit
public class nbsi_tester extends TestCase {
  public static void main(String args[]) {
    org.junit.runner.JUnitCore.main("nbsi_tester");
  }

  private NonBlockingSetInt _nbsi;
  protected void setUp   () { _nbsi = new NonBlockingSetInt(100); }
  protected void tearDown() { _nbsi = null; }

  // Test some basic stuff; add a few keys, remove a few keys
  public void testBasic() {
    assertTrue ( _nbsi.isEmpty() );
    assertTrue ( _nbsi.add(1) );
    checkSizes (1);
    assertTrue ( _nbsi.add(2) );
    checkSizes (2);
    assertFalse( _nbsi.add(1) );
    assertFalse( _nbsi.add(2) );
    checkSizes (2);
    assertThat ( _nbsi.remove(1), is(true ) );
    checkSizes (1);
    assertThat ( _nbsi.remove(1), is(false) );
    assertTrue ( _nbsi.remove(2) );
    checkSizes (0);
    assertFalse( _nbsi.remove(2) );
    assertFalse( _nbsi.remove(3) );
    assertTrue ( _nbsi.isEmpty() );
  }

  // Check all iterators for correct size counts
  private void checkSizes(int expectedSize) {
    assertEquals( "size()", _nbsi.size(), expectedSize );
    Iterator it = _nbsi.iterator();
    int result = 0;
    while (it.hasNext()) {
      result++;
      it.next();
    }
    assertEquals( "iterator missed", expectedSize, result );
  }


  public void testIteration() {
    assertTrue ( _nbsi.isEmpty() );
    assertTrue ( _nbsi.add(1) );
    assertTrue ( _nbsi.add(2) );

    String str1 = "";
    for( Iterator<Integer> i = _nbsi.iterator(); i.hasNext(); ) {
      Integer val = i.next();
      str1 += val;
    }
    assertThat("found all vals",str1,anyOf(is("12"),is("21")));

    assertThat("toString works",_nbsi.toString(), anyOf(is("[1, 2]"),is("[2, 1]")));
  }

  public void testIterationBig() {
    for( int i=0; i<100; i++ )
      _nbsi.add(i);
    assertThat( _nbsi.size(), is(100) );

    int sz =0;
    int sum = 0;
    for( Integer x : _nbsi ) {
      sz++;
      sum += x;
      assertTrue(x>=0 && x<=99);
    }
    assertThat("Found 100 ints",sz,is(100));
    assertThat("Found all integers in list",sum,is(100*99/2));

    assertThat( "can remove 3", _nbsi.remove(3), is(true) );
    assertThat( "can remove 4", _nbsi.remove(4), is(true) );
    sz =0;
    sum = 0;
    for( Integer x : _nbsi ) {
      sz++;
      sum += x;
      assertTrue(x>=0 && x<=99);
    }
    assertThat("Found 98 ints",sz,is(98));
    assertThat("Found all integers in list",sum,is(100*99/2 - (3+4)));

  }

  public void testSerial() {
    assertTrue ( _nbsi.isEmpty() );
    assertTrue ( _nbsi.add(1) );
    assertTrue ( _nbsi.add(2) );

    // Serialize it out
    try {
      FileOutputStream fos = new FileOutputStream("NBSI_test.txt");
      ObjectOutputStream out = new ObjectOutputStream(fos);
      out.writeObject(_nbsi);
      out.close();
    } catch(IOException ex) {
      ex.printStackTrace();
    }

    // Read it back
    try {
      FileInputStream fis = new FileInputStream("NBSI_test.txt");
      ObjectInputStream in = new ObjectInputStream(fis);
      NonBlockingSetInt nbsi = (NonBlockingSetInt)in.readObject();
      in.close();
      assertEquals(_nbsi.toString(),nbsi.toString());
    } catch(IOException ex) {
      ex.printStackTrace();
    } catch(ClassNotFoundException ex) {
      ex.printStackTrace();
    }
  }

  // Do some simple concurrent testing
  public void testConcurrentSimple() throws InterruptedException {
    final NonBlockingSetInt nbsi = new NonBlockingSetInt(1000);
    
    // In 2 threads, add & remove even & odd elements concurrently
    Thread t = new Thread() {
        public void run() {
          for( int j=0; j<100; j++ ) {
            for( int i=0; i<1000; i+=2 )
              nbsi.add(i);
            for( int i=0; i<1000; i+=2 )
              nbsi.remove(i);
          }
        }
      };
    t.start();
    for( int j=0; j<100; j++ ) {
      for( int i=1; i<1000; i+=2 )
        nbsi.add(i);
      for( int i=1; i<1000; i+=2 )
        nbsi.remove(i);
    }
    t.join();

    // In the end, all members should be removed
    assertThat( "concurrent size=0", nbsi.size(), is(0) );
    for( Integer x : nbsi ) {
      assertTrue("No elements so never get here",false);
    }

  }
}
