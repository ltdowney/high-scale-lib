/*
 * Written by Cliff Click and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 * Additional test cases provided by Andy Martin of TeleAtlas.
 */

import java.io.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import junit.framework.TestCase;
import org.cliffc.high_scale_lib.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

// Test NonBlockingHashMapLong via JUnit
public class NBHML_Tester2 extends TestCase {
  public static void main(String args[]) {
    org.junit.runner.JUnitCore.main("NBHML_Tester2");
  }

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
      FileOutputStream fos = new FileOutputStream("NBHML_test.txt");
      ObjectOutputStream out = new ObjectOutputStream(fos);
      out.writeObject(_nbhml);
      out.close();
    } catch(IOException ex) {
      ex.printStackTrace();
    }

    // Read it back
    try {
      File f = new File("NBHML_test.txt");
      FileInputStream fis = new FileInputStream(f);
      ObjectInputStream in = new ObjectInputStream(fis);
      NonBlockingHashMapLong nbhml = (NonBlockingHashMapLong)in.readObject();
      in.close();
      assertEquals(_nbhml.toString(),nbhml.toString());
      f.delete();
    } catch(IOException ex) {
      ex.printStackTrace();
    } catch(ClassNotFoundException ex) {
      ex.printStackTrace();
    }

  }

  public void testIterationBig() {
    final int CNT = 10000;
    assertThat( _nbhml.size(), is(0) );
    for( int i=0; i<CNT; i++ ) 
      _nbhml.put(i,"v"+i);
    assertThat( _nbhml.size(), is(CNT) );

    int sz =0;
    int sum = 0;
    for( long x : _nbhml.keySet() ) {
      sz++;
      sum += x;
      assertTrue(x>=0 && x<=(CNT-1));
    }
    assertThat("Found 10000 ints",sz,is(CNT));
    assertThat("Found all integers in list",sum,is(CNT*(CNT-1)/2));

    assertThat( "can remove 3", _nbhml.remove(3), is("v3") );
    assertThat( "can remove 4", _nbhml.remove(4), is("v4") );
    sz =0;
    sum = 0;
    for( long x : _nbhml.keySet() ) {
      sz++;
      sum += x;
      assertTrue(x>=0 && x<=(CNT-1));
      String v = _nbhml.get(x);
      assertThat("",v.charAt(0),is('v'));
      assertThat("",x,is(Long.parseLong(v.substring(1))));
    }
    assertThat("Found "+(CNT-2)+" ints",sz,is(CNT-2));
    assertThat("Found all integers in list",sum,is(CNT*(CNT-1)/2 - (3+4)));
  }

  // Do some simple concurrent testing
  public void testConcurrentSimple() throws InterruptedException {
    final NonBlockingHashMapLong<String> nbhml = new NonBlockingHashMapLong<String>();

    // In 2 threads, add & remove even & odd elements concurrently
    Thread t1 = new Thread() { public void run() { work_helper(nbhml,"T1",1); } };
    t1.start();
    work_helper(nbhml,"T0",0);
    t1.join();

    // In the end, all members should be removed
    StringBuffer buf = new StringBuffer();
    buf.append("Should be emptyset but has these elements: {");
    boolean found = false;
    for( long x : nbhml.keySet() ) {
      buf.append(" ").append(x);
      found = true;
    }
    if( found ) System.out.println(buf+" }");
    assertThat( "concurrent size=0", nbhml.size(), is(0) );
    for( long x : nbhml.keySet() ) {
      assertTrue("No elements so never get here",false);
    }
  }

  void work_helper(NonBlockingHashMapLong<String> nbhml, String thrd, int d) {
    final int ITERS = 20000;
    for( int j=0; j<10; j++ ) {
      long start = System.nanoTime();
      for( int i=d; i<ITERS; i+=2 )
        assertThat( "this key not in there, so putIfAbsent must work", 
                    nbhml.putIfAbsent(i,thrd), is((String)null) );
      for( int i=d; i<ITERS; i+=2 )
        assertTrue( nbhml.remove(i,thrd) );
      double delta_nanos = System.nanoTime()-start;
      double delta_secs = delta_nanos/1000000000.0;
      double ops = ITERS*2;
      //System.out.println("Thrd"+thrd+" "+(ops/delta_secs)+" ops/sec size="+nbhml.size());
    }
  }


  // --- Customer Test Case 1 ------------------------------------------------
  public final void testNonBlockingHashMapSize() {
    NonBlockingHashMapLong<String> items = new NonBlockingHashMapLong<String>();
    items.put(Long.valueOf(100), "100");
    items.put(Long.valueOf(101), "101");
    
    assertEquals("keySet().size()", 2, items.keySet().size());
    assertTrue("keySet().contains(100)", items.keySet().contains(Long.valueOf(100)));
    assertTrue("keySet().contains(101)", items.keySet().contains(Long.valueOf(101)));
    
    assertEquals("values().size()", 2, items.values().size());
    assertTrue("values().contains(\"100\")", items.values().contains("100"));
    assertTrue("values().contains(\"101\")", items.values().contains("101"));
    
    assertEquals("entrySet().size()", 2, items.entrySet().size());
    boolean found100 = false;
    boolean found101 = false;
    for (Entry<Long, String> entry : items.entrySet()) {
      if (entry.getKey().equals(Long.valueOf(100))) {
        assertEquals("entry[100].getValue()==\"100\"", "100", entry.getValue());
        found100 = true;
      } else if (entry.getKey().equals(Long.valueOf(101))) {
        assertEquals("entry[101].getValue()==\"101\"", "101", entry.getValue());
        found101 = true;
      }
    }
    assertTrue("entrySet().contains([100])", found100);
    assertTrue("entrySet().contains([101])", found101);
  }
  
  // --- Customer Test Case 2 ------------------------------------------------
  // Concurrent insertion & then iterator test.
  static public void testNonBlockingHashMapIterator() throws InterruptedException {
    final int ITEM_COUNT1 = 1000;
    final int THREAD_COUNT = 5;
    final int PER_CNT = ITEM_COUNT1/THREAD_COUNT;
    final int ITEM_COUNT = PER_CNT*THREAD_COUNT; // fix roundoff for odd thread counts
  
    NonBlockingHashMapLong<TestKey> nbhml = new NonBlockingHashMapLong<TestKey>();
    // use a barrier to open the gate for all threads at once to avoid rolling
    // start and no actual concurrency
    final CyclicBarrier barrier = new CyclicBarrier(THREAD_COUNT);
    final ExecutorService ex = Executors.newFixedThreadPool(THREAD_COUNT);
    final CompletionService<Object> co = new ExecutorCompletionService<Object>(ex);
    for( int i=0; i<THREAD_COUNT; i++ )
        co.submit(new NBHMLFeeder(nbhml, PER_CNT, barrier, i*PER_CNT));
    for( int retCount = 0; retCount < THREAD_COUNT; retCount++ )
      co.take();
    
    assertEquals("values().size()", ITEM_COUNT, nbhml.values().size());
    assertEquals("entrySet().size()", ITEM_COUNT, nbhml.entrySet().size());
    int itemCount = 0;
    for( TestKey K : nbhml.values() )
      itemCount++;
    assertEquals("values().iterator() count", ITEM_COUNT, itemCount);
  }

  // --- NBHMLFeeder ---
  // Class to be called from another thread, to get concurrent installs into
  // the table.
  static private class NBHMLFeeder implements Callable<Object> {
    static private final Random _rand = new Random(System.currentTimeMillis());
    private final NonBlockingHashMapLong<TestKey> _map;
    private final int _count;
    private final CyclicBarrier _barrier;
    private final long _offset;
    public NBHMLFeeder(final NonBlockingHashMapLong<TestKey> map, final int count, final CyclicBarrier barrier, final long offset) {
      _map = map;
      _count = count;
      _barrier = barrier;
      _offset = offset;
    }
    @Override
    public Object call() throws Exception {
      _barrier.await();         // barrier, to force racing start
      for( long j=0; j<_count; j++ )
        _map.put(j+_offset, new TestKey(_rand.nextLong(),_rand.nextInt (), (short) _rand.nextInt(Short.MAX_VALUE)));
      return null;
    }
  }

  // --- TestKey ---
  // Funny key tests all sorts of things, has a pre-wired hashCode & equals.
  static private final class TestKey {
    // seeds for FNV hash
    static private final int HASH_PRIME = 0x1000193;
    static private final int HASH_SEED = (0x811C9DC5 * HASH_PRIME) ^ TestKey.class.hashCode();
    // class members
    public final short _s;
    public final int   _i;
    public final long  _l;
    public final int _hash;
    public TestKey(final long l, final int i, final short s) {
      _l = l;
      _i = i;
      _s = s;
      // calculate hash one time since the key is going to be used in maps and
      // hashCode() may be called many times
      _hash = ((((HASH_SEED * HASH_PRIME) ^ (int)(_l >>> 32) *
                 HASH_PRIME) ^ (int)(_l) *
                HASH_PRIME) ^ _s *
               HASH_PRIME) ^ _i;
    }
    @Override public int hashCode() { return _hash;  }
    @Override public boolean equals(Object object) {
      if (null == object) return false;
      if (object == this) return true;
      if (object.getClass() != this.getClass()) return false;
      final TestKey other = (TestKey) object;
      return (this._l == other._l && this._i == other._i && this._s == other._s);
    }
    
  }
}
