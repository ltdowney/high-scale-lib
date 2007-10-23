/*
 * Written by Cliff Click and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

package org.cliffc.high_scale_lib;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import sun.misc.Unsafe;
import java.lang.reflect.*;
//import com.azulsystems.util.Prefetch;

public class NonBlockingHashMapLong<TypeV> 
  extends AbstractMap<Long,TypeV> 
  implements ConcurrentMap<Long,TypeV>, Serializable {

  private static final long serialVersionUID = 1234123412341234124L;

  private static final int REPROBE_LIMIT=10; // Too many reprobes then force a table-resize

  // --- Bits to allow Unsafe access to arrays
  private static final Unsafe _unsafe = UtilUnsafe.getUnsafe();
  private static final int _Obase  = _unsafe.arrayBaseOffset(Object[].class);
  private static final int _Oscale = _unsafe.arrayIndexScale(Object[].class);
  private static long rawIndex(final Object[] ary, final int idx) {
    assert idx >= 0 && idx < ary.length;
    return _Obase + idx * _Oscale;
  }
  private static final int _Lbase  = _unsafe.arrayBaseOffset(long[].class);
  private static final int _Lscale = _unsafe.arrayIndexScale(long[].class);
  private static long rawIndex(final long[] ary, final int idx) {
    assert idx >= 0 && idx < ary.length;
    return _Lbase + idx * _Lscale;
  }

  // --- Bits to allow Unsafe CAS'ing of the CHM field
  private static final long _chm_offset;
  private static final long _val_1_offset;
  private static final long _val_2_offset;
  static {                      // <clinit>
    Field f = null;
    try { f = NonBlockingHashMapLong.class.getDeclaredField("_chm"); } 
    catch( java.lang.NoSuchFieldException e ) { throw new RuntimeException(e); } 
    _chm_offset = _unsafe.objectFieldOffset(f);

    try { f = NonBlockingHashMapLong.class.getDeclaredField("_val_1"); } 
    catch( java.lang.NoSuchFieldException e ) { throw new RuntimeException(e); } 
    _val_1_offset = _unsafe.objectFieldOffset(f);

    try { f = NonBlockingHashMapLong.class.getDeclaredField("_val_2"); } 
    catch( java.lang.NoSuchFieldException e ) { throw new RuntimeException(e); } 
    _val_2_offset = _unsafe.objectFieldOffset(f);
  }
  private final boolean CAS( final long offset, final Object old, final Object nnn ) {
    return _unsafe.compareAndSwapObject(this, offset, old, nnn );
  }

  // --- Adding a 'prime' bit onto Values via wrapping with a junk wrapper class
  public static final class Prime {
    public final Object _V;
    Prime( Object V ) { _V = V; }
    static Object unbox( Object V ) { return V instanceof Prime ? ((Prime)V)._V : V;  }
  }

  // --- hash ----------------------------------------------------------------
  // Helper function to spread lousy hashCodes
  private static final int hash(long h) {
    h ^= (h>>>20) ^ (h>>>12);
    h ^= (h>>> 7) ^ (h>>> 4);
    return (int)h;
  }

  // --- The Hash Table
  public transient CHM _chm;
  // These next 2 keys allow me to have special sentinel values for some long
  // Keys, especially key 0 which is the default value for a new Java array.
  // These key/value slots exist always, but the key is assumed.  The Value is
  // either null (means empty) or a true Value, but not a TOMBSTONE.  Since
  // these two keys exist in all tables there's no need for a
  // CHECK_NEW_TABLE_SENTINEL value.  
  public TypeV _val_1;          // Value for Key: NO_KEY
  public TypeV _val_2;          // Value for Key: CHECK_NEW_TABLE_SENTINEL

  // Size in active K,V pairs in all nested CHM's
  private transient ConcurrentAutoTable _size;

  // --- Some misc minimum table size and Sentiel
  private static final int MIN_SIZE_LOG=5;
  private static final int MIN_SIZE=(1<<MIN_SIZE_LOG); // Must be power of 2

  private static final long NO_KEY = 0L;
  private static final long CHECK_NEW_TABLE_LONG = -1L;

  private static final Object CHECK_NEW_TABLE_SENTINEL = new Object(); // Sentinel
  private static final Object NO_MATCH_OLD = new Object(); // Sentinel
  private static final Object TOMBSTONE = new Object();
  private static final Prime  TOMBPRIME = new Prime(TOMBSTONE);

  // --- dump ----------------------------------------------------------------
  public final void dump() { 
    System.out.println("=========");
    if( _val_1 != null ) dump_impl(-99,NO_KEY,              _val_1);
    if( _val_2 != null ) dump_impl(-99,CHECK_NEW_TABLE_LONG,_val_2);
    _chm.dump();
    System.out.println("=========");
  }
  private static final void dump_impl(final int i, final long K, final Object V) { 
    String KS = (K == CHECK_NEW_TABLE_LONG    ) ? "CHK" : ""+K;
    String p = V instanceof Prime ? "prime_" : "";
    String VS = (V == CHECK_NEW_TABLE_SENTINEL) ? "CHK" : (p+Prime.unbox(V));
    if( Prime.unbox(V) == TOMBSTONE ) VS = "tombstone";
    System.out.println(""+i+" ("+KS+","+VS+")");
  }
    
  public final void dump2() { 
    System.out.println("=========");
    if( _val_1 != null ) dump2_impl(-99,NO_KEY,              _val_1);
    if( _val_2 != null ) dump2_impl(-99,CHECK_NEW_TABLE_LONG,_val_2);
    _chm.dump();
    System.out.println("=========");
  }
  private static final void dump2_impl(final int i, final long K, final Object V) { 
    if( V != null && V != CHECK_NEW_TABLE_SENTINEL && V != TOMBSTONE ) {
      String p = V instanceof Prime ? "prime_" : "";
      String VS = p+Prime.unbox(V);
      System.out.println(""+i+" ("+K+","+VS+")");
    }
  }

  // --- check ---------------------------------------------------------------
  // Internal consistency check
  public final void check() {
  }

  // --- NonBlockingHashMap --------------------------------------------------
  // Constructors
  public NonBlockingHashMapLong( ) { this(MIN_SIZE); }
  public NonBlockingHashMapLong( final int initial_sz ) { 
    if( initial_sz < 0 ) throw new IllegalArgumentException();
    int i;                      // Convert to next largest power-of-2
    for( i=MIN_SIZE_LOG; (1<<i) < initial_sz; i++ ) ;
    _chm = new CHM(this,i);
    _size = new ConcurrentAutoTable();
  }

  // --- wrappers ------------------------------------------------------------
  public int     size       ( )                     { return (_val_1==null?0:1) + (_val_2==null?0:1) + (int)_size.sum(); }
  public boolean isEmpty    ( )                     { return size()==0; }
  public boolean containsKey( long key )            { return get(key) != null; }
  public TypeV   put        ( long key, TypeV val ) { return (TypeV)putIfMatch( key,  val, NO_MATCH_OLD );  }
  public TypeV   putIfAbsent( long key, TypeV val ) { return (TypeV)putIfMatch( key,  val, null );  }
  public TypeV   remove     ( long key )            { return (TypeV)putIfMatch( key, null, NO_MATCH_OLD );  }
  public boolean remove     ( long key, Object val ){ return        putIfMatch( key, null,  val ) == val; }
  public boolean replace    ( long key, TypeV  oldValue, TypeV newValue ) {
    if (oldValue == null || newValue == null)  throw new NullPointerException();
    return putIfMatch( key, newValue, oldValue ) == oldValue;
  }
  public TypeV replace( long key, TypeV val ) {
    if (val == null)  throw new NullPointerException();
    return putIfAbsent( key, val );
  }
  public boolean containsValue( Object val ) { 
    if( val == null ) throw new NullPointerException();
    if( _val_1 == val ) return true; // Key -1
    if( _val_2 == val ) return true; // Key -2
    return _chm.contains(val); 
  }
  public boolean contains( Object val ) { return containsValue(val); }
  public void clear() {         // Smack a new empty table down
    CHM newchm = new CHM(this,MIN_SIZE_LOG);
    while( !CAS(_chm_offset,_chm,newchm) ) // Spin until the clear works
      ;
    CAS(_val_1_offset,_val_1,null);
    CAS(_val_2_offset,_val_2,null);
  }

  private final Object putIfMatch( Object curVal, TypeV val, Object expVal, long off ) {
    if( expVal == NO_MATCH_OLD || // Do we care about expected-Value at all?
        curVal == expVal ||       // No instant match already?
        (expVal != null && expVal.equals(curVal)) ) // Expensive equals check
      CAS(off,curVal,val);        // One shot CAS update attempt
    return curVal;                // Return the last value present
  }

  private final Object putIfMatch( long key, TypeV val, Object oldVal ) {
    if( key == NO_KEY               ) return putIfMatch(_val_1,val,oldVal,_val_1_offset);
    if( key == CHECK_NEW_TABLE_LONG ) return putIfMatch(_val_2,val,oldVal,_val_2_offset);
    Object newval = val;
    if( newval == null ) newval = TOMBSTONE;
    if( oldVal == null ) oldVal = TOMBSTONE;
    Object res = _chm.putIfMatch( key, newval, oldVal );
    assert !(res instanceof Prime);
    return res == TOMBSTONE ? null : res;
  }

  // Get!  Can return 'null' to mean Tombstone or empty
  public final TypeV get( long key ) {
    if( key == NO_KEY               ) return _val_1;
    if( key == CHECK_NEW_TABLE_LONG ) return _val_2;
    Object V = _chm.get_impl(key);
    assert !(V instanceof Prime); // No prime in main oldest table
    return V == TOMBSTONE ? null : (TypeV)V;
  }

  public TypeV   get    ( Object key              ) { return (key instanceof Long) ? get    (((Long)key).longValue()) : null;  }
  public TypeV   remove ( Object key              ) { return (key instanceof Long) ? remove (((Long)key).longValue()) : null;  }
  public boolean remove ( Object key, Object Val  ) { return (key instanceof Long) ? remove (((Long)key).longValue(), Val) : false;  }
  public TypeV   replace( Long key, TypeV Val     ) { return (key instanceof Long) ? replace(((Long)key).longValue(), Val) : null;  }
  public boolean replace( Long key, TypeV oldValue, TypeV newValue ) { return (key instanceof Long) ? replace(((Long)key).longValue(), oldValue, newValue) : false;  }
  public TypeV   putIfAbsent( Long key, TypeV val ) { return (key instanceof Long) ? (TypeV)putIfMatch( ((Long)key).longValue(),  val, null ) : null;  }
  public boolean containsKey( Object key          ) { return (key instanceof Long) ? containsKey(((Long)key).longValue()) : false; }
  public TypeV put( Long key, TypeV val ) { return put(key.longValue(),val); }

  // --- help_copy ---------------------------------------------------------
  // Help along an existing resize operation.  This is just a fast cut-out
  // wrapper, to encourage inlining for the fast no-copy-in-progress case.
  private final void help_copy( ) {
    final CHM topchm = _chm;
    if( topchm._newchm != null )
      topchm.help_copy_impl(false);
  }
 

  // --- CHM -----------------------------------------------------------------
  private static final class CHM<TypeV> implements Serializable {
    final NonBlockingHashMapLong _nbhm;

    // These next 2 fields are used in the resizing heuristics, to judge when
    // it is time to resize or copy the table.  Slots is a count of used-up
    // key slots, and when it nears a large fraction of the table we probably
    // end up reprobing too much.  Last-resize-milli is the time since the
    // last resize; if we are running back-to-back resizes without growing
    // (because there are only a few live keys but many slots full of dead
    // keys) then we need a larger table to cut down on the churn.

    // Count of used slots, to tell when table is full of dead unusable slots
    private final ConcurrentAutoTable _slots;
    private int slots() { return (int)_slots.sum(); }
    // Time since last resize
    private long _last_resize_milli;
    
    // New mappings, used during resizing.
    // The 'next' CHM - created during a resize operation.  This represents
    // the new table being copied from the old one.  It monotonically transits
    // from null to set (once).
    volatile CHM _newchm;
    private static final AtomicReferenceFieldUpdater<CHM,CHM> _newchmUpdater =
      AtomicReferenceFieldUpdater.newUpdater(CHM.class,CHM.class, "_newchm");
    // Set the _newchm field if we can.
    boolean CAS_newchm( CHM newchm ) { 
      while( _newchm == null ) 
        if( _newchmUpdater.compareAndSet(this,null,newchm) )
          return true;
      return false;
    }
    // Sometimes many threads race to create a new very large table.  Only 1
    // wins the race, but the losers all allocate a junk large table with
    // hefty allocation costs.  Attempt to control the overkill here by
    // throttling attempts to create a new table.  I cannot really block here
    // (lest I lose the non-blocking property) but late-arriving threads can
    // give the initial resizing thread a little time to allocate the initial
    // new table.  The Right Long Term Fix here is to use array-lets and
    // incrementally create the new very large array.  In C I'd make the array
    // with malloc (which would mmap under the hood) which would only eat
    // virtual-address and not real memory - and after Somebody wins then we
    // could in parallel initialize the array.  Java does not allow
    // un-initialized array creation (especially of ref arrays!).
    volatile long _resizers;    // count of threads attempting an initial resize
    private static final AtomicLongFieldUpdater<CHM> _resizerUpdater =
      AtomicLongFieldUpdater.newUpdater(CHM.class, "_resizers");

    // --- key,val -------------------------------------------------------------
    // Access K,V for a given idx
    private final boolean CAS_key( int idx, long   old, long   key ) {
      return _unsafe.compareAndSwapLong  ( _keys, rawIndex(_keys, idx), old, key );
    }
    private final boolean CAS_val( int idx, Object old, Object val ) {
      return _unsafe.compareAndSwapObject( _vals, rawIndex(_vals, idx), old, val );
    }

    final long   [] _keys;
    final Object [] _vals;
   
    // Simple constructor
    CHM( NonBlockingHashMapLong nbhm, int logsize ) {
      _nbhm = nbhm;
      _slots= new ConcurrentAutoTable();
      _last_resize_milli = System.currentTimeMillis();
      _keys = new long  [1<<logsize];
      _vals = new Object[1<<logsize];
    }

    // --- dump innards
    private final void dump() { 
      for( int i=0; i<_keys.length; i++ ) {
        long K = _keys[i];
        if( K != NO_KEY )
          dump_impl(i,K,_vals[i]);
      }
      CHM newchm = _newchm;     // New table, if any
      if( newchm != null ) {
        System.out.println("----");
        newchm.dump();
      }
    }

    // --- dump only the live objects
    private final void dump2( ) { 
      for( int i=0; i<_keys.length; i++ ) {
        long K = _keys[i];
        if( K != NO_KEY && K != CHECK_NEW_TABLE_LONG )// key is sane
          dump2_impl(i,K,_vals[i]);
      }
      CHM newchm = _newchm;     // New table, if any
      if( newchm != null ) {
        System.out.println("----");
        newchm.dump2();
      }
    }

    // --- contains ------------------------------------------------------------
    // Search for matching value.
    public final boolean contains( Object val ) {
      for( int i=0; i<_vals.length; i++ ) { // Otherwise hunt the hard way
        Object V = Prime.unbox(_vals[i]);
        if( V == val || val.equals(V) )
          return true;
      }
      CHM newchm = _newchm;
      return newchm == null ? false : newchm.contains(val);
    }

    // --- get -----------------------------------------------------------------
    private final Object get_recur( long key ) {
      return Prime.unbox(get_impl(key));
    }
    private final Object get_impl ( long key ) {
      final int fullhash = hash((int)key);
      final int len = _keys.length;
      int hash = fullhash & (len-1); // First key hash
      
      // Main spin/reprobe loop, looking for a Key hit
      int reprobe_cnt=0;
      while( true ) {
        final long   K = _keys[hash]; // First key
        final Object V = _vals[hash]; // Get value, could be Tombstone/empty or sentinel
        if( K == NO_KEY ) return null; // A clear miss
        if( K==key ) {            // Key hit?
          if( V != CHECK_NEW_TABLE_SENTINEL ) {
            Object vol = _newchm; // dummy Read of Volatile
            return V;
          }
          break;                  // Got a key hit, but wrong table!
        }
        // get and put must have the same key lookup logic!  But only 'put'
        // needs to force a table-resize for a too-long key-reprobe sequence.
        // Check for reprobes on get.
        if( K == CHECK_NEW_TABLE_LONG ||
            ++reprobe_cnt >= (REPROBE_LIMIT + (len>>2)) )
          // This is treated as a MISS in this table.  If there is a new table,
          // retry in that table and since we had to indirect to get there -
          // assist in the copy to remove the old table (lest we get stuck
          // paying indirection on every lookup).
          break;
        hash = (hash+1)&(len-1); // Reprobe by 1!  (should force a prefetch)
      }
      
      // Found table-copy sentinel; retry the get on the new table then help copy
      CHM chm = _newchm; // New table, if any; this counts as the volatile read needed between tables
      if( chm == null ) return null; // No new table, so a clean miss.
      _nbhm.help_copy();
      return chm.get_recur(key); // Retry on the new table
    }
  
    // --- putIfMatch ---------------------------------------------------------
    // Put, Remove, PutIfAbsent, etc.  Return the old value.  If the old value
    // is equal to oldVal (or oldVal is NO_MATCH_OLD) then the put can be
    // assumed to work (although might have been immediately overwritten).
    private final Object putIfMatch( long key, Object putval, Object expVal ) {
      assert !(putval instanceof Prime);
      assert !(expVal instanceof Prime);
      assert putval != null;
      assert expVal != null;
      final int fullhash = hash((int)key);
      final int len = _keys.length;
      int hash = fullhash & (len-1);
      
      // ---
      // Key-Claim stanza: spin till we can claim a Key (or force a resizing).
      boolean assert_goto_new_table = false;
      int reprobe_cnt=0;
      long   K = NO_KEY;
      Object V = null;
      while( true ) {           // Spin till we get it
        V = _vals[hash];        // Get old value
        K = _keys[hash];        // Get current key
        if( K == NO_KEY ) {     // Slot is free?
          if( putval == TOMBSTONE ) return null; // Not-now & never-been in this table
          // If the table is getting full I do not want to install a new key in
          // this old table - instead end all key chains and fall into the next
          // code, which will move on to the new table.
          if( tableFull(reprobe_cnt,len) && // Table is full?
              CAS_key(hash,NO_KEY,CHECK_NEW_TABLE_LONG) ) {
            assert_goto_new_table = true; // for asserts only
            copy_done(1);                 // nuked an old-table slot, so some copy-work is done
          }
          if( CAS_key(hash, NO_KEY, key) ) { // Claim slot for Key
            _slots.add(1);                 // Raise slot count
            break;                // Got it!
          }
          K = _keys[hash];        // CAS failed, get updated value
          assert K != NO_KEY ;    // If keys[hash] is NO_KEY, CAS shoulda worked
        }
        if( K==key )
          break;                  // Got it!
        
        // get and put must have the same key lookup logic!  Lest 'get' give
        // up looking too soon.
        if( K == CHECK_NEW_TABLE_LONG ||
            ++reprobe_cnt >= (REPROBE_LIMIT + (len>>2)) ) {
          // We simply must have a new table to do a 'put'.  At this point a
          // 'get' will also go to the new table (if any).  We do not need
          // to claim a key slot (indeed, we cannot find a free one to claim!).
          _nbhm.help_copy();
          return resize().putIfMatch(key,putval,expVal);
        }      
        hash = (hash+1)&(len-1); // Reprobe!
      }
      
      // ---
      // Found the proper Key slot, now update the matching Value slot
      if( putval == V ) return V; // Fast cutout for no-change
      
      // See if we want to move to a new table.
      // It is OK to force a new table "early", because the put in the new
      // table will override anything in the old table - so any get following
      // this put will see the sentinel and move to the new table.
      if( (_newchm != null ||   // Table copy already in progress?
           tableFull(reprobe_cnt,len)) ) { // Or table is full?
        // New table is forced if table is full & not resize already in progress
        CHM newchm = resize();
        // Copy this slot in the old table to the new table.  This copy can
        // CAS spin, however, the number of times the CAS can fail is bounded
        // by the number of late-arriving 'put' operations who don't realize a
        // table copy is in progress (i.e., limited by thread count).
        copy_one_done(hash);
        // Help any top-level copy along
        _nbhm.help_copy();
        // Now put into the new table
        return newchm.putIfMatch(key,putval,expVal);
      }
      assert !assert_goto_new_table; // tableFull should still be true, so we should not get here!
      
      // ---
      assert V != CHECK_NEW_TABLE_SENTINEL; // Resizing Sentinel?
      if( V instanceof Prime ) V = ((Prime)V)._V; // Unbox
      
      // Must match old, and we do not?  Then bail out now.
      if( expVal != NO_MATCH_OLD && // Do we care about expected-Value at all?
          V != expVal &&          // No instant match already?
          !(V==null && expVal == TOMBSTONE) &&  // Match on null/TOMBSTONE combo
          !expVal.equals(V) )     // Expensive equals check
        return V;
      
      // Actually change the Value in the Key,Value pair
      if( CAS_val(hash, V, putval ) ) { // Note: no looping on this CAS failing
        // CAS succeeded - we did the update!
        // Adjust sizes - a striped counter
        if(  (V == null || V == TOMBSTONE) && putval != TOMBSTONE ) _nbhm._size.add( 1);
        if( !(V == null || V == TOMBSTONE) && putval == TOMBSTONE ) _nbhm._size.add(-1);
      }
      // Win or lose the CAS, we are done.  If we won then we know the update
      // happened as expected.  If we lost, it means "we won but another thread
      // immediately stomped our update with no chance of a reader reading".
      return V; 
    }
    
    // --- tableFull ---------------------------------------------------------

    // Heuristic to decide if this table is too full, and we should start a
    // new table.  Note that if a 'get' call has reprobed too many times and
    // decided the table must be full, then always the estimate_sum must be
    // high and we must report the table is full.  If we do not, then we might
    // end up deciding that the table is not full and inserting into the
    // current table, while a 'get' has decided the same key cannot be in this
    // table because of too many reprobes.  The invariant is:
    //   slots.estimate_sum >= max_reprobe_cnt >= REPROBE_LIMIT+(len>>2)
    private final boolean tableFull( int reprobe_cnt, int len ) {
      return 
        // Do the cheap check first: we allow some number of reprobes always
        reprobe_cnt >= REPROBE_LIMIT &&
        // More expensive check: see if the table is > 1/4 full.
        _slots.estimate_sum() >= REPROBE_LIMIT+(len>>2);
    }

    // --- resize ------------------------------------------------------------
    // Resizing after too many probes.  "How Big???" heuristics are here.
    private final CHM resize() {
      CHM newchm;
      while( true ) {
        // Check for resize already in progress, probably triggered by another thread
        newchm = _newchm;
        if( newchm != null )    // See if resize is already in progress
          return newchm;        // Use the new table already
        // If we are a nested table, force the top-level table to finish resizing
        CHM topchm = _nbhm._chm;
        if( topchm == this ) break;  // We ARE the top-level table
        topchm.help_copy_impl(true); // Force the top-level table resize to finish
      }
      
      int oldlen = _keys.length; // Old count of K,V pairs allowed
      assert slots() >= REPROBE_LIMIT+(oldlen>>2); // No change in size needed?
      int sz = _nbhm.size(); // Get current table count of active K,V pairs
      int newsz = sz;           // First size estimate

      // Heuristic to determine new size.  We expect plenty of dead-slots-with-keys 
      // and we need some decent padding to avoid endless reprobing.
      if( sz >= (oldlen>>2) ) { // If we are >25% full of keys then...
        newsz = oldlen<<1;      // Double size
        if( sz >= (oldlen>>1) ) // If we are >50% full of keys then...
          newsz = oldlen<<2;    // Double double size
      }

      // Last (re)size operation was very recent?  Then double again; slows
      // down resize operations for tables subject to a high key churn rate.
      long tm = System.currentTimeMillis();
      if( newsz <= oldlen && tm <= _last_resize_milli+1000 ) 
        newsz = oldlen<<1;      // Double in size again
      _last_resize_milli = tm;

      // Do not shrink, ever.
      if( newsz < oldlen ) newsz = oldlen;

      // Convert to power-of-2
      int log2;
      for( log2=MIN_SIZE_LOG; (1<<log2) < newsz; log2++ ) ; // Compute log2 of size

      long r = _resizers;
      while( !_resizerUpdater.compareAndSet(this,r,r+1) )
        r = _resizers;
      int megs = ((((1<<log2)<<1)+4)<<3/*word to bytes*/)>>20/*megs*/;
      if( r >= 2 && megs > 0 ) { // Already 2 guys trying; wait and see
        newchm = _newchm;
        if( newchm != null )    // See if resize is already in progress
          return newchm;        // Use the new table already
        // TODO - use a wait with timeout, so we'll wakeup as soon as the new table
        // is ready, or after the timeout in any case.
        //synchronized( this ) { wait(8*megs); }         // Timeout - we always wakeup
        try { Thread.sleep(8*megs); } catch( Exception e ) { }
      }
      // Last check, since the 'new' below is expensive and there is a chance
      // that another thread slipped in a new thread while we ran the heuristic.
      newchm = _newchm;
      if( newchm != null )      // See if resize is already in progress
        return newchm;          // Use the new table already

      // Double size for K,V pairs
      newchm = new CHM(_nbhm,log2);
      // Another check after the slow allocation
      if( _newchm != null )     // See if resize is already in progress
        return _newchm;         // Use the new table already

      // The new table must be CAS'd in so only 1 winner amongst duplicate
      // racing resizing threads.  Extra CHM's will be GC'd.
      //long nano = System.nanoTime();
      if( CAS_newchm( newchm ) ) { // Now a resize-is-in-progress!
        //notifyAll();            // Wake up any sleepers
        //System.out.println(" "+nano+" Resize from "+oldlen+" to "+(1<<log2)+" and had "+(_resizers-1)+" extras" );
      } else                    // CAS failed?
        newchm = _newchm;       // Reread new table
      return newchm;
    }


    // The next part of the table to copy.  It monotonically transits from zero
    // to _keys.length.  Visitors to the table can claim 'work chunks' by
    // CAS'ing this field up, then copying the indicated indices from the old
    // table to the new table.  Workers are not required to finish any chunk;
    // the counter simply wraps and work is copied duplicately until somebody
    // somewhere completes the count.
    volatile long _copyIdx = 0;
    private static final AtomicLongFieldUpdater<CHM> _copyIdxUpdater =
      AtomicLongFieldUpdater.newUpdater(CHM.class, "_copyIdx");

    // Work-done reporting.  Used to efficiently signal when we can move to
    // the new table.  From 0 to len(oldkvs) refers to copying from the old
    // table to the new.
    volatile long _copyDone= 0;
    private static final AtomicLongFieldUpdater<CHM> _copyDoneUpdater =
      AtomicLongFieldUpdater.newUpdater(CHM.class, "_copyDone");

    // --- help_copy_impl ----------------------------------------------------
    // Help along an existing resize operation.
    private final void help_copy_impl( final boolean copy_all ) {
      int oldlen = _keys.length;
      final int MIN_COPY_WORK = oldlen == 32 ? 16 : (oldlen == 64 ? 32 : 64);
      CHM newchm = _newchm;

      // Do copy-work first
      int copyidx = (int)_copyIdx;
      int panicidx = -1;
      while( _copyDone < oldlen ) { // Still needing to copy?
        // Carve out a chunk of work.  The counter wraps around so every
        // thread eventually tries to copy every slot.
        while( true ) {
          int ci = (int)_copyIdx; // Read start of next unclaimed work chunk
          if( ci >= (oldlen<<1) ) { // Panic?
            if( panicidx < 0 ) { // Panic!  Finish the copy ourself
              panicidx = copyidx;
              //long nano = System.nanoTime();
              //System.out.println(" "+nano+" Panic start at "+panicidx);
            }
            break;              // Panic!  Just keep copying from here to forever...
          }
          if( _copyIdxUpdater.compareAndSet(this,ci,ci+MIN_COPY_WORK) ) {
            copyidx = ci;       // Got some work
            break;
          }
        }
        
        // Try to copy some slots
        int workdone = 0;
        for( int i=0; i<MIN_COPY_WORK; i++ )
          if( copy_one((copyidx+i)&(oldlen-1)) ) // Made an oldtable slot go dead?
            workdone++;         // Yes!
            
        if( workdone > 0 ) {    // Report work-done occasionally
          copy_done( workdone );
          if( panicidx == -1 && !copy_all ) 
            return; // If not forcing copy-all, then get out!  We did our share of copy-work
        }
        copyidx += MIN_COPY_WORK;
        if( panicidx+oldlen <= copyidx )
          break;                // Panic-copied the whole array?
      }
    }

    
    // --- copy_one_done -----------------------------------------------------
    // Wrap a single copy-attempt-and-test 
    private final void copy_one_done( final int idx ) {
      if( copy_one(idx) )
        copy_done(1);
    }

    // --- copy_done ---------------------------------------------------------
    // Some copy-work got done.  Add it up and see if we are all done;
    // if so then promote 'this' to become the top-level hash map.
    private final void copy_done( final int workdone ) {
      // We made a slot unusable and so did some of the needed copy work
      long copyDone = _copyDone;
      while( !_copyDoneUpdater.compareAndSet(this,copyDone,copyDone+workdone) ) 
        copyDone = _copyDone;   // Reload, retry
      if( copyDone+workdone > _keys.length )
        throw new RuntimeException("too much copyDone:"+copyDone+" work="+workdone+" > len="+_keys.length);
      assert (copyDone+workdone) <= _keys.length;
      // Check for copy being ALL done, and promote
      if( copyDone+workdone == _keys.length ) {
        //long nano = System.nanoTime();
        if( _nbhm.CAS(_chm_offset,this,_newchm) ) { // Promote!
          //System.out.println(" "+nano+" Promote table to "+len(_newkvs));
        }
      }
    }    

    // --- copy_one ----------------------------------------------------------
    // Copy one K/V pair from 'this' to _newchm.  Returns true if we slammed
    // a CHECK_NEW_TABLE_SENTINEL in this older table.  After some mental
    // debate I decided that this routine will loop until the value is copied,
    // instead of giving up on conflict.  This means that a poor helper thread
    // might be stuck spinning until obstructing threads finish doing updates
    // but those are limited to 1 late-arriving update per thread.
    private final boolean copy_one( int idx ) {
      long key = _keys[idx];            // Read old table
      if( key == CHECK_NEW_TABLE_LONG ) // slot already dead?
        return false;           // Slot dead but we did not do it
      if( key == NO_KEY ) {     // Try to kill a dead slot
        if( CAS_key(idx, NO_KEY, CHECK_NEW_TABLE_LONG ) )
          return true;          // We made slot go dead
        // CAS from NO_KEY-to-CHECK_NEW failed.  Check for slot already dead
        key = _keys[idx];       // Reload after failed CAS
        assert key != NO_KEY;
        if( key == CHECK_NEW_TABLE_LONG ) // slot already dead?
          return false;                   // Slot dead but we did not do it
      }
      CHM newchm = _newchm;                // New table
      final int fullhash = hash(key);
      final int len = newchm._keys.length; // Count of key/value pairs in new table
      int hash = fullhash & (len-1);

      // ---
      // Key-Claim stanza: spin till we can claim a Key (or force a resizing).
      int cnt=0;
      while( true ) {                // Spin till we get key slot in new table
        long K = newchm._keys[hash]; // Get new table key
        if( K == NO_KEY ) {          // Slot is free?
          Object V = _vals[idx];     // Read OLD table
          assert !(V instanceof Prime);
          if( V == CHECK_NEW_TABLE_SENTINEL ) 
            return false;       // Dead in old, not in new, so copy complete
          // Not in old table, not in new table, and no need for it in new table.
          if( V == null || V == TOMBSTONE ) { 
            if( CAS_val(idx, null, CHECK_NEW_TABLE_SENTINEL ) ) // Try to wipe it out now.
              return true;
          }
          // Claim new-table slot for key!
          if( newchm.CAS_key(hash, NO_KEY, key ) ) { // Claim slot for Key
            newchm._slots.add(1); // Raise slot count
            break;                // Got it!
          }
          K = newchm._keys[hash]; // CAS failed, get updated value
          assert K != NO_KEY ;  // If keys[hash] is NO_KEY, CAS shoulda worked
        }
        if( K == key )
          break;                // Got it!

        if( ++cnt >= (REPROBE_LIMIT+(len>>2)) ) {
          Object V = _vals[idx]; // Read OLD table
          if( V == CHECK_NEW_TABLE_SENTINEL ) 
            return false;       // Dead in old, not in new, so copy complete
          // Still in old table, but no space in new???
          long nano = System.nanoTime();
          long slots= newchm.slots();
          System.out.println(""+nano+" copy oldslot="+idx+"/"+_keys.length+" K="+K+" no_slot="+cnt+"/"+len+" slots="+slots+" live="+_nbhm.size()+"");
          throw new RuntimeException("some kind of table copy resizing error");
        }
        hash = (hash+1)&(len-1); // Reprobe!
      }
      // We have a Key-slot in the new table now, most likely a {Key,null} pair

      // ---
      // Spin until we complete the copy
      Object newV;
      boolean did_work = false;
      while( true ) {
        newV = newchm._vals[hash];
        final Object dummy = _newchm; // dummy volatile read
        final Object oldV = _vals[idx];
        assert !(oldV instanceof Prime);
        assert oldV != null;    // Not sure if this assert is too strong

        // Is our work done here?  Old slot is smacked so no more copy?
        if( oldV == CHECK_NEW_TABLE_SENTINEL )
          break;

        // Check for a mismatch between old and new.  Update the new table
        // with a Primed version of the old table value.
        if( (newV instanceof Prime && ((Prime)newV)._V != oldV) || newV == null ) {
          final Prime old_primeV = oldV == TOMBSTONE ? TOMBPRIME : new Prime(oldV);
          if( !newchm.CAS_val(hash, newV, old_primeV ) )
            continue;           // Failed CAS?  Try to copy again
          newV = old_primeV;    // CAS worked, so newV is a prime copy of oldV
        }
        // newV is now a prime version of oldV, or perhaps newV is not prime at all
        assert( !(newV instanceof Prime) || ((Prime)newV)._V == oldV );
        // Complete copy by killing old slot with a CHECK_NEW
        if( CAS_val(idx, oldV, CHECK_NEW_TABLE_SENTINEL ) ) {
          did_work = true;
          break;
        }
      } // end spin-until-copy-completes

      // Now clear out the Prime bit from the new table
      if( newV instanceof Prime ) {
        if( !newchm.CAS_val( hash, newV, ((Prime)newV)._V ) )
          assert( !(newchm._vals[hash] instanceof Prime) );
      }
      return did_work;
    } // end copy_one
  } // End CHM class

  // --- Snapshot ------------------------------------------------------------
  class SnapshotV implements Iterator<TypeV> {
    final CHM _chm;
    public SnapshotV(CHM chm) { _chm = chm; _idx = -2; next(); }
    int length() { return _chm._keys.length; }
    long key(int idx) { return _chm._keys[idx]; }
    private int _idx;           // -2 for NO_KEY, -1 for CHECK_NEW_TABLE_LONG, 0-keys.length
    private long  _nextK, _prevK; // Last 2 keys found
    private TypeV _nextV, _prevV; // Last 2 values found
    public boolean hasNext() { return _nextV != null; }
    public TypeV next() {
      // 'next' actually knows what the next value will be - it had to
      // figure that out last go 'round lest 'hasNext' report true and
      // some other thread deleted the last value.  Instead, 'next'
      // spends all its effort finding the key that comes after the
      // 'next' key.
      if( _idx != -2 && _nextV == null ) throw new NoSuchElementException();
      _prevK = _nextK;          // This will become the previous key
      _prevV = _nextV;          // This will become the previous value
      _nextV = null;            // We have no more next-key
      // Attempt to set <_nextK,_nextV> to the next K,V pair.
      // _nextV is the trigger: stop searching when it is != null
      if( _idx == -2 ) {        // Check for NO_KEY?
        _idx = -1;              // Setup for next phase of search
        _nextK = NO_KEY;  
        if( (_nextV=get(_nextK)) != null ) return _prevV;
      }
      if( _idx == -1 ) {        // Check for CHECK_NEW_TABLE_LONG?
        _idx = 0;               // Setup for next phase of search
        _nextK = CHECK_NEW_TABLE_LONG;  _nextV = get(_nextK);
        if( (_nextV=get(_nextK)) != null ) return _prevV;
      }
      while( _idx<length() ) {  // Scan array
        _nextK = key(_idx++); // Get a key that definitely is in the set (for the moment!)
        if( _nextK != NO_KEY && // Found something?
            _nextK != CHECK_NEW_TABLE_LONG &&
            (_nextV=get(_nextK)) != null )
          break;                // Got it!  _nextK is a valid Key
      }                         // Else keep scanning
      return _prevV;            // Return current value.
    }
    public void remove() { 
      if( _prevV == null ) throw new IllegalStateException();
      _chm._nbhm.remove(_prevK);
      _prevV = null;
    }
  }

  // --- values --------------------------------------------------------------
  public Collection<TypeV> values() {
    return new AbstractCollection<TypeV>() {
      public void    clear   (          ) {        NonBlockingHashMapLong.this.clear   ( ); }
      public int     size    (          ) { return NonBlockingHashMapLong.this.size    ( ); }
      public boolean contains( Object v ) { return NonBlockingHashMapLong.this.containsValue(v); }
      public Iterator<TypeV> iterator()   { return new SnapshotV(_chm); }
    };
  }

  // --- keySet --------------------------------------------------------------
  class SnapshotK implements Iterator<Long> {
    final SnapshotV _ss;
    public SnapshotK(CHM chm) { _ss = new SnapshotV(chm); }
    public void remove() { _ss.remove(); }
    public Long next() { _ss.next(); return _ss._prevK; }
    public boolean hasNext() { return _ss.hasNext(); }
  }
  public Set<Long> keySet() {
    return new AbstractSet<Long> () {
      public void    clear   (          ) {        NonBlockingHashMapLong.this.clear   ( ); }
      public int     size    (          ) { return NonBlockingHashMapLong.this.size    ( ); }
      public boolean contains( Object k ) { return NonBlockingHashMapLong.this.containsKey(k); }
      public boolean remove  ( Object k ) { return NonBlockingHashMapLong.this.remove  (k) != null; }
      public Iterator<Long> iterator()    { return new SnapshotK(_chm); }
    };
  }

  // --- entrySet ------------------------------------------------------------
  // Warning: Each call to 'next' in this iterator constructs a new Long and a
  // new WriteThroughEntry.
  class NBHMLEntry extends AbstractEntry<Long,TypeV> {
    NBHMLEntry( final Long k, final TypeV v ) { super(k,v); }
    public TypeV setValue(TypeV val) {
      if (val == null) throw new NullPointerException();
      _val = val;
      return put(_key, val);
    }
  }
  class SnapshotE implements Iterator<Map.Entry<Long,TypeV>> {
    final SnapshotV _ss;
    public SnapshotE(CHM chm) { _ss = new SnapshotV(chm); }
    public void remove() { _ss.remove(); }
    public Map.Entry<Long,TypeV> next() { _ss.next(); return new NBHMLEntry(_ss._prevK,_ss._prevV); }
    public boolean hasNext() { return _ss.hasNext(); }
  }
  public Set<Map.Entry<Long,TypeV>> entrySet() {
    return new AbstractSet<Map.Entry<Long,TypeV>>() {
      public void    clear   (          ) {        NonBlockingHashMapLong.this.clear( ); }
      public int     size    (          ) { return NonBlockingHashMapLong.this.size ( ); }
      public boolean remove( Object o ) {
        if (!(o instanceof Map.Entry)) return false;
        Map.Entry<?,?> e = (Map.Entry<?,?>)o;
        return NonBlockingHashMapLong.this.remove(e.getKey(), e.getValue());
      }
      public boolean contains(Object o) {
        if (!(o instanceof Map.Entry)) return false;
        Map.Entry<?,?> e = (Map.Entry<?,?>)o;
        TypeV v = NonBlockingHashMapLong.this.get(e.getKey());
        return v != null && v.equals(e.getValue());
      }
      public Iterator<Map.Entry<Long,TypeV>> iterator() { return new SnapshotE(_chm); }
    };
  }

  // --- writeObject -------------------------------------------------------
  // Write a NBMHL to a stream
  private void writeObject(java.io.ObjectOutputStream s) throws IOException  {
    s.defaultWriteObject();     // Write just val1 & val2
    final long[] keys = _chm._keys;
    for( int i=0; i<keys.length; i++ ) {
      final long K = keys[i];
      if( K != NO_KEY && K != CHECK_NEW_TABLE_LONG ) { // Only serialize keys in this table
        final Object V = get(K); // But do an official 'get' in case key is being copied
        if( V != null ) {     // Key might have been deleted
          s.writeLong  (K);   // Write the <long,TypeV> pair
          s.writeObject(V);
        }
      }
    }
    s.writeLong(NO_KEY);      // Sentinel to indicate end-of-data
    s.writeObject(null);
  }
  
  // --- readObject --------------------------------------------------------
  // Read a CHM from a stream
  private void readObject(java.io.ObjectInputStream s) throws IOException, ClassNotFoundException  {
    s.defaultReadObject();      // Read val1 & val2
    _chm = new CHM(this,MIN_SIZE_LOG);
    _size = new ConcurrentAutoTable();
    for (;;) {
      final long key = s.readLong();
      final TypeV V = (TypeV) s.readObject();
      if( key == NO_KEY ) break;
      put(key,V);               // Insert with an offical put
    }
  }
  
}  // End NonBlockingHashMapLong class
