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
  static {                      // <clinit>
    Field f = null;
    try { f = NonBlockingHashMapLong.class.getDeclaredField("_chm"); }
    catch( java.lang.NoSuchFieldException e ) { throw new RuntimeException(e); } 
    _chm_offset = _unsafe.objectFieldOffset(f);

    try { f = NonBlockingHashMapLong.class.getDeclaredField("_val_1"); }
    catch( java.lang.NoSuchFieldException e ) { throw new RuntimeException(e); } 
    _val_1_offset = _unsafe.objectFieldOffset(f);
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
  private static final int hash(final long h) {
    h ^= (h>>>20) ^ (h>>>12);
    h ^= (h>>> 7) ^ (h>>> 4);
    return (int)h;
  }

  // --- The Hash Table --------------------
  public transient CHM _chm;
  // This next field holds the value for Key 0 - the special key value which
  // is the initial array value, and also means: no-key-inserted-yet.
  public TypeV _val_1;          // Value for Key: NO_KEY

  // Time since last resize
  private transient long _last_resize_milli;

  // --- Minimum table size ----------------
  // Pick size 16 K/V pairs, which turns into (16*2)*4+12 = 140 bytes on a
  // standard 32-bit HotSpot, and (16*2)*8+12 = 268 bytes on 64-bit Azul.
  private static final int MIN_SIZE_LOG=4;             // 
  private static final int MIN_SIZE=(1<<MIN_SIZE_LOG); // Must be power of 2

  // --- Sentinels -------------------------
  // No-Match-Old - putIfMatch does updates only if it matches the old value,
  // and NO_MATCH_OLD basically counts as a wildcard match.
  public static final Object NO_MATCH_OLD = new Object(); // Sentinel
  // This K/V pair has been deleted (but the Key slot is forever claimed).
  // The same Key can be reinserted with a new value later.
  public static final Object TOMBSTONE = new Object();
  // Prime'd or box'd version of TOMBSTONE.  This K/V pair was deleted, then a
  // table resize started.  The K/V pair has been marked so that no new
  // updates can happen to the old table (and since the K/V pair was deleted
  // nothing was copied to the new table).
  public static final Prime  TOMBPRIME = new Prime(TOMBSTONE);

  // I exclude 1 long from the 2^64 possibilities, and test for it before
  // entering the main array.  The NO_KEY value must be zero, the initial
  // value set by Java before it hands me the array.
  private static final long NO_KEY = 0L;

  // --- dump ----------------------------------------------------------------
  public final void dump() { 
    System.out.println("=========");
    if( _val_1 != null ) dump_impl(-99,NO_KEY,_val_1);
    _chm.dump();
    System.out.println("=========");
  }
  private static final void dump_impl(final int i, final long K, final Object V) { 
    String p = (V instanceof Prime) ? "prime_" : "";
    Object V2 = Prime.unbox(V);
    String VS = (V2 == TOMBSTONE) ? "tombstone" : V2;
    System.out.println("["+i+"]=("+K+","+p+VS+")");
  }
    
  public final void dump2() { 
    System.out.println("=========");
    if( _val_1 != null ) dump2_impl(-99,NO_KEY,_val_1);
    _chm.dump();
    System.out.println("=========");
  }
  private static final void dump2_impl(final int i, final long K, final Object V) { 
    if( V != null && Prime.unbox(V) != TOMBSTONE )
      dump_impl(i,K,V);
  }

  // Count of reprobes
  private transient ConcurrentAutoTable _reprobes = new ConcurrentAutoTable();
  public long reprobes() { long r = _reprobes.sum(); _reprobes = new ConcurrentAutoTable(); return r; }


  // --- reprobe_limit -----------------------------------------------------
  // Heuristic to decide if we have reprobed toooo many times.  Running over
  // the reprobe limit on a 'get' call acts as a 'miss'; on a 'put' call it
  // can trigger a table resize.  Several places must have exact agreement on
  // what the reprobe_limit is, so we share it here.
  private static final int reprobe_limit( int len ) {
    return REPROBE_LIMIT + (len>>2);
  }

  // --- NonBlockingHashMapLong ----------------------------------------------
  // Constructors
  public NonBlockingHashMapLong( ) { this(MIN_SIZE); }
  public NonBlockingHashMapLong( final int initial_sz ) { initialize(initial_sz); }
  void initialize(int initial_sz ) { 
    if( initial_sz < 0 ) throw new IllegalArgumentException();
    int i;                      // Convert to next largest power-of-2
    for( i=MIN_SIZE_LOG; (1<<i) < initial_sz; i++ ) ;
    _chm = new CHM(this,i);
    _last_resize_milli = System.currentTimeMillis();
  }

  // --- wrappers ------------------------------------------------------------
  public int     size       ( )                     { return (_val_1==null?0:1) + (int)_chm.size(); }
  public boolean isEmpty    ( )                     { return size()==0; }
  public boolean containsKey( long key )            { return get(key) != null; }
  public boolean contains   ( Object val )          { return containsValue(val); }
  public TypeV   put        ( long key, TypeV val ) { return (TypeV)putIfMatch( key,      val,NO_MATCH_OLD);}
  public TypeV   putIfAbsent( long key, TypeV val ) { return (TypeV)putIfMatch( key,      val,TOMBSTONE   );}
  public TypeV   remove     ( long key )            { return (TypeV)putIfMatch( key,TOMBSTONE,NO_MATCH_OLD);}
  public boolean remove     ( long key, Object val ){ 
    return putIfMatch( key, TOMBSTONE, (val==null)?TOMBSTONE:val ) == val; 
  }
  public boolean replace    ( long key, TypeV  oldValue, TypeV newValue ) {
    if (oldValue == null || newValue == null)  throw new NullPointerException();
    return putIfMatch( key, newValue, oldValue ) == oldValue;
  }
  public TypeV replace( long key, TypeV val ) {
    if (val == null)  throw new NullPointerException();
    return putIfAbsent( key, val );
  }
  private final Object putIfMatch( Object curVal, TypeV val, Object expVal, long off ) {
    if( expVal == NO_MATCH_OLD || // Do we care about expected-Value at all?
        curVal == expVal ||       // No instant match already?
        (expVal != null && expVal.equals(curVal)) ) // Expensive equals check
      CAS(off,curVal,val);        // One shot CAS update attempt
    return curVal;                // Return the last value present
  }

  private final Object putIfMatch( long key, Object newVal, Object oldVal ) {
    if( key == NO_KEY ) {
      final Object curVal = _val_1;
      if( oldVal == NO_MATCH_OLD || // Do we care about expected-Value at all?
          curVal == oldVal ||       // No instant match already?
          (oldVal != null && oldVal.equals(curVal)) ) // Expensive equals check
        CAS(_val_1_offset,curVal,newVal); // One shot CAS update attempt
      return curVal;                // Return the last value present
    }
    assert newVal != null;
    assert oldVal != null;
    final Object res = _chm.putIfMatch( key, newVal, oldVal );
    assert !(res instanceof Prime);
    assert res != null;
    return res == TOMBSTONE ? null : res;
  }

  public void putAll(Map<Long, ? extends TypeV> t) {
    Iterator<? extends Map.Entry<Long, ? extends TypeV>> i = t.entrySet().iterator();
    while (i.hasNext()) {
      Map.Entry<Long, ? extends TypeV> e = i.next();
      put(e.getKey(), e.getValue());
    }
  }

  // Atomically replace the CHM with a new empty CHM
  public void clear() {         // Smack a new empty table down
    CHM newchm = new CHM(this,MIN_SIZE_LOG);
    while( !CAS(_chm_offset,_chm,newchm) ) // Spin until the clear works
      ;
    CAS(_val_1_offset,_val_1,null);
  }

  public boolean containsValue( Object val ) { 
    if( val == null ) return false;
    if( val == _val_1 ) return true; // Key 0
    for( TypeV V : values() )
      if( V == val || V.equals(val) )
        return true;
    return false;
  }

  // --- get -----------------------------------------------------------------
  // Get!  Returns 'null' to mean Tombstone or empty.  
  // Never returns a Prime nor a Tombstone.
  public final TypeV get( long key ) {
    if( key == NO_KEY ) return _val_1;
    final Object V = _chm.get_impl(key);
    assert !(V instanceof Prime); // Never return a Prime
    return (TypeV)V;
  }

  public TypeV   get    ( Object key              ) { return (key instanceof Long) ? get    (((Long)key).longValue()) : null;  }
  public TypeV   remove ( Object key              ) { return (key instanceof Long) ? remove (((Long)key).longValue()) : null;  }
  public boolean remove ( Object key, Object Val  ) { return (key instanceof Long) ? remove (((Long)key).longValue(), Val) : false;  }
  public TypeV   replace( Long key, TypeV Val     ) { return (key instanceof Long) ? replace(((Long)key).longValue(), Val) : null;  }
  public boolean replace( Long key, TypeV oldValue, TypeV newValue ) { return (key instanceof Long) ? replace(((Long)key).longValue(), oldValue, newValue) : false;  }
  public TypeV   putIfAbsent( Long key, TypeV val ) { return (key instanceof Long) ? (TypeV)putIfMatch( ((Long)key).longValue(),  val, null ) : null;  }
  public boolean containsKey( Object key          ) { return (key instanceof Long) ? containsKey(((Long)key).longValue()) : false; }
  public TypeV put( Long key, TypeV val ) { return put(key.longValue(),val); }

  // --- help_copy -----------------------------------------------------------
  // Help along an existing resize operation.  This is just a fast cut-out
  // wrapper, to encourage inlining for the fast no-copy-in-progress case.  We
  // always help the top-most table copy, even if there are nested table
  // copies in progress.
  private final void help_copy( ) {
    // Read the top-level CHM only once.  We'll try to help this copy along,
    // even if it gets promoted out from under us (i.e., the copy completes
    // and another KVS becomes the top-level copy).
    CHM topchm = _chm;
    if( topchm._newchm == null ) return; // No copy in-progress
    topchm.help_copy_impl(false);
  }


  // --- CHM -----------------------------------------------------------------
  // The control structure for the NonBlockingHashMapLong
  private static final class CHM<TypeV> implements Serializable {
    // Back-pointer to top-level structure
    final NonBlockingHashMapLong _nbhml;

    // Size in active K,V pairs
    private final ConcurrentAutoTable _size;
    public int size () { return (int)_size.sum(); }

    // ---
    // These next 2 fields are used in the resizing heuristics, to judge when
    // it is time to resize or copy the table.  Slots is a count of used-up
    // key slots, and when it nears a large fraction of the table we probably
    // end up reprobing too much.  Last-resize-milli is the time since the
    // last resize; if we are running back-to-back resizes without growing
    // (because there are only a few live keys but many slots full of dead
    // keys) then we need a larger table to cut down on the churn.

    // Count of used slots, to tell when table is full of dead unusable slots
    private final ConcurrentAutoTable _slots;
    public int slots() { return (int)_slots.sum(); }
    
    // ---
    // New mappings, used during resizing.
    // The 'next' CHM - created during a resize operation.  This represents
    // the new table being copied from the old one.  It's the volatile
    // variable that is read as we cross from one table to the next, to get
    // the required memory orderings.  It monotonically transits from null to
    // set (once).
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
    CHM( final NonBlockingHashMapLong nbhml, final int logsize ) {
      _nbhml = nbhml;
      _slots= new ConcurrentAutoTable();
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
        if( K != NO_KEY )       // key is sane
          dump2_impl(i,K,_vals[i]);
      }
      CHM newchm = _newchm;     // New table, if any
      if( newchm != null ) {
        System.out.println("----");
        newchm.dump2();
      }
    }

    // --- get_impl ----------------------------------------------------------
    // Never returns a Prime nor a Tombstone.
    private final Object get_impl ( final long key ) {
      final int fullhash= hash(key);
      final int len     = _keys.length;

      int idx = fullhash & (len-1); // First key hash

      // Main spin/reprobe loop, looking for a Key hit
      int reprobe_cnt=0;
      while( true ) {
        final long   K = _keys[idx]; // Get key   before volatile read, could be NO_KEY
        final Object V = _vals[idx]; // Get value before volatile read, could be null or Tombstone or Prime
        if( K == NO_KEY ) return null; // A clear miss

        // Key-compare
        if( key == K ) {
          // Key hit!  Check for no table-copy-in-progress
          if( !(V instanceof Prime) ) { // No copy?
            if( V == TOMBSTONE) return null;
            // We need a volatile-read between reading a newly inserted Value
            // and returning the Value (so the user might end up reading the
            // stale Value contents).
            final Object[] newkvs = chm._newkvs; // VOLATILE READ before returning V
            return V;
          }
          // Key hit - but slot is (possibly partially) copied to the new table.
          // Finish the copy & retry in the new table.
          return copy_slot_and_check(idx).get_impl(key); // Retry in the new table
        }
        // get and put must have the same key lookup logic!  But only 'put'
        // needs to force a table-resize for a too-long key-reprobe sequence.
        // Check for too-many-reprobes on get.
        //topmap._reprobes.add(1);
        if( ++reprobe_cnt >= reprobe_limit(len) ) // too many probes
          return null;            // This is treated as a MISS in this table.
        
        idx = (idx+1)&(len-1);    // Reprobe by 1!  (could now prefetch)
      }
    }
  
    // --- putIfMatch ---------------------------------------------------------
    // Put, Remove, PutIfAbsent, etc.  Return the old value.  If the returned
    // value is equal to expVal (or expVal is NO_MATCH_OLD) then the put can
    // be assumed to work (although might have been immediately overwritten).
    // Only the path through copy_slot passes in an expected value of null,
    // and putIfMatch only returns a null if passed in an expected null.
    private final Object putIfMatch( final long key, final Object putval, final Object expVal ) {
      assert putval != null;
      assert !(putval instanceof Prime);
      assert !(expVal instanceof Prime);
      final int fullhash = hash  (key); // throws NullPointerException if key null
      final int len      = _keys.length;
      int idx = fullhash & (len-1);

      // ---
      // Key-Claim stanza: spin till we can claim a Key (or force a resizing).
      int reprobe_cnt=0;
      long   K = NO_KEY;
      Object V = null;
      CHM newchm = null;
      while( true ) {           // Spin till we get a Key slot
        V = _vals[idx];         // Get old value
        K = _keys[idx];         // Get current key
        if( K == NO_KEY ) {     // Slot is free?
          // Found an empty Key slot - which means this Key has never been in
          // this table.  No need to put a Tombstone - the Key is not here!
          if( putval == TOMBSTONE ) return putval; // Not-now & never-been in this table
          // Claim the zero key-slot
          if( CAS_key(idx, NO_KEY, key) ) { // Claim slot for Key
            _slots.add(1);      // Raise key-slots-used count
            break;              // Got it!
          }
          // CAS to claim the key-slot failed.
          //
          // This re-read of the Key points out an annoying short-coming of Java
          // CAS.  Most hardware CAS's report back the existing value - so that
          // if you fail you have a *witness* - the value which caused the CAS
          // to fail.  The Java API turns this into a boolean destroying the
          // witness.  Re-reading does not recover the witness because another
          // thread can write over the memory after the CAS.  Hence we can be in
          // the unfortunate situation of having a CAS fail *for cause* but
          // having that cause removed by a later store.  This turns a
          // non-spurious-failure CAS (such as Azul has) into one that can
          // apparently spuriously fail - and we avoid apparent spurious failure
          // by not allowing Keys to ever change.
          K = _keys[idx];       // CAS failed, get updated value
          assert K != NO_KEY ;  // If keys[idx] is NO_KEY, CAS shoulda worked
        }
        // Key slot was not null, there exists a Key here
        if( K == key )
          break;                // Got it!
      
        // get and put must have the same key lookup logic!  Lest 'get' give
        // up looking too soon.  
        //topmap._reprobes.add(1);
        if( ++reprobe_cnt >= reprobe_limit(len) ) {
          // We simply must have a new table to do a 'put'.  At this point a
          // 'get' will also go to the new table (if any).  We do not need
          // to claim a key slot (indeed, we cannot find a free one to claim!).
          newchm = resize();
          if( expVal != null ) _nbhml.help_copy(); // help along an existing copy
          return newchm.putIfMatch(key,putval,expVal);
        }
        
        idx = (idx+1)&(len-1); // Reprobe!
      } // End of spinning till we get a Key slot
      
      // ---
      // Found the proper Key slot, now update the matching Value slot.  We
      // never put a null, so Value slots monotonically move from null to
      // not-null (deleted Values use Tombstone).  Thus if 'V' is null we
      // fail this fast cutout and fall into the check for table-full.
      if( putval == V ) return V; // Fast cutout for no-change

      // See if we want to move to a new table (to avoid high average re-probe
      // counts).  We only check on the initial set of a Value from null to
      // not-null (i.e., once per key-insert).  Of course we got a 'free' check
      // of newchm once per key-compare (not really free, but paid-for by the
      // time we get here).
      if( newchm == null &&       // New table-copy already spotted?
          // Once per fresh key-insert check the hard way
          ((V == null && tableFull(reprobe_cnt,len)) ||
           // Or we found a Prime, but the JMM allowed reordering such that we
           // did not spot the new table (very rare race here: the writing
           // thread did a CAS of _newchm then a store of a Prime.  This thread
           // reads the Prime, then reads _newchm - but the read of Prime was so
           // delayed (or the read of _newchm was so accelerated) that they
           // swapped and we still read a null _newchm.  The resize call below
           // will do a CAS on _newchm forcing the read.
           V instanceof Prime) ) {
        if( V instanceof Prime ) 
          throw new Error("Untested: very rare race with reordering reads of Prime with reads of _newchm");
        newchm = resize(); // Force the new table copy to start
      }
      // See if we are moving to a new table.  
      // If so, copy our slot and retry in the new table.
      if( newchm != null )
        return copy_slot_and_check(idx).putIfMatch(key,putval,expVal);
      
      // ---
      // We are finally prepared to update the existing table
      assert !(V instanceof Prime);
      
      // Must match old, and we do not?  Then bail out now.  Note that either V
      // or expVal might be TOMBSTONE.  Also V can be null, if we've never
      // inserted a value before.  expVal can be null if we are called from
      // copy_slot.

      if( expVal != NO_MATCH_OLD && // Do we care about expected-Value at all?
          V != expVal &&          // No instant match already?
          !(V==null && expVal == TOMBSTONE) &&  // Match on null/TOMBSTONE combo
          (expVal == null || !expVal.equals(V)) ) // Expensive equals check at the last
        return expVal;            // Do not update!

      // Actually change the Value in the Key,Value pair
      if( CAS_val(idx, V, putval ) ) {
        // CAS succeeded - we did the update!
        // Both normal put's and table-copy calls putIfMatch, but table-copy
        // does not (effectively) increase the number of live k/v pairs.
        if( expVal != null ) {
          // Adjust sizes - a striped counter
          if(  (V == null || V == TOMBSTONE) && putval != TOMBSTONE ) chm._size.add( 1);
          if( !(V == null || V == TOMBSTONE) && putval == TOMBSTONE ) chm._size.add(-1);
        }
      } else {                    // Else CAS failed
        V = val(kvs,idx);         // Get new value
        // If a Prime'd value got installed, we need to re-run the put on the
        // new table.  Otherwise we lost the CAS to another racing put.
        // Simply retry from the start.
        if( V instanceof Prime )
          return putIfMatch(key,putval,expVal);
      }
      // Win or lose the CAS, we are done.  If we won then we know the update
      // happened as expected.  If we lost, it means "we won but another thread
      // immediately stomped our update with no chance of a reader reading".
      return (V==null && expVal!=null) ? TOMBSTONE : V;
    }
    
    // --- tableFull ---------------------------------------------------------
    // Heuristic to decide if this table is too full, and we should start a
    // new table.  Note that if a 'get' call has reprobed too many times and
    // decided the table must be full, then always the estimate_sum must be
    // high and we must report the table is full.  If we do not, then we might
    // end up deciding that the table is not full and inserting into the
    // current table, while a 'get' has decided the same key cannot be in this
    // table because of too many reprobes.  The invariant is:
    //   slots.estimate_sum >= max_reprobe_cnt >= reprobe_limit(len)
    private final boolean tableFull( int reprobe_cnt, int len ) {
      return 
        // Do the cheap check first: we allow some number of reprobes always
        reprobe_cnt >= REPROBE_LIMIT &&
        // More expensive check: see if the table is > 1/4 full.
        _slots.estimate_sum() >= reprobe_limit(len);
    }

    // --- resize ------------------------------------------------------------
    // Resizing after too many probes.  "How Big???" heuristics are here.
    // Callers will (not this routine) will 'help_copy' any in-progress copy.
    // Since this routine has a fast cutout for copy-already-started, callers
    // MUST 'help_copy' lest we have a path which forever runs through
    // 'resize' only to discover a copy-in-progress which never progresses.
    private final CHM resize() {
      // Check for resize already in progress, probably triggered by another thread
      Object[] newchm = _newchm; // VOLATILE READ
      if( newchm != null )       // See if resize is already in progress
        return newchm;           // Use the new table already

      // No copy in-progress, so start one.  First up: compute new table size.
      int oldlen = _keys.length); // Old count of K,V pairs allowed
      int sz = size();          // Get current table count of active K,V pairs
      int newsz = sz;           // First size estimate

      // Heuristic to determine new size.  We expect plenty of dead-slots-with-keys 
      // and we need some decent padding to avoid endless reprobing.
      if( sz >= (oldlen>>2) ) { // If we are >25% full of keys then...
        newsz = oldlen<<1;      // Double size
        if( sz >= (oldlen>>1) ) // If we are >50% full of keys then...
          newsz = oldlen<<2;    // Double double size
      }
      // This heuristic in the next 2 lines leads to a much denser table
      // with a higher reprobe rate
      //if( sz >= (oldlen>>1) ) // If we are >50% full of keys then...
      //  newsz = oldlen<<1;    // Double size

      // Last (re)size operation was very recent?  Then double again; slows
      // down resize operations for tables subject to a high key churn rate.
      long tm = System.currentTimeMillis();
      long q=0;
      if( newsz <= oldlen &&    // New table would shrink or hold steady?
          tm <= topmap._last_resize_milli+10000 && // Recent resize (less than 1 sec ago)
          (q=_slots.estimate_sum()) >= (sz<<1) ) // 1/2 of keys are dead?
        newsz = oldlen<<1;      // Double the existing size

      // Do not shrink, ever
      if( newsz < oldlen ) newsz = oldlen;

      // Convert to power-of-2
      int log2;
      for( log2=MIN_SIZE_LOG; (1<<log2) < newsz; log2++ ) ; // Compute log2 of size

      // Now limit the number of threads actually allocating memory to a
      // handful - lest we have 750 threads all trying to allocate a giant
      // resized array.
      long r = _resizers;
      while( !_resizerUpdater.compareAndSet(this,r,r+1) )
        r = _resizers;
      // Size calculation: 2 words (K+V) per table entry, plus a handful.  We
      // guess at 32-bit pointers; 64-bit pointers screws up the size calc by
      // 2x but does not screw up the heuristic very much.
      int megs = ((((1<<log2)<<1)+4)<<3/*word to bytes*/)>>20/*megs*/;
      if( r >= 2 && megs > 0 ) { // Already 2 guys trying; wait and see
        newchm = _newchm;        // Between dorking around, another thread did it
        if( newchm != null )     // See if resize is already in progress
          return newchm;         // Use the new table already
        // TODO - use a wait with timeout, so we'll wakeup as soon as the new table
        // is ready, or after the timeout in any case.
        //synchronized( this ) { wait(8*megs); }         // Timeout - we always wakeup
        // For now, sleep a tad and see if the 2 guys already trying to make
        // the table actually get around to making it happen.
        try { Thread.sleep(8*megs); } catch( Exception e ) { }
      }
      // Last check, since the 'new' below is expensive and there is a chance
      // that another thread slipped in a new thread while we ran the heuristic.
      newchm = _newchm;
      if( newchm != null )      // See if resize is already in progress
        return newchm;          // Use the new table already

      // Double size for K,V pairs, add 1 for CHM
      newchm = new CHM(_nbhml,log2)
      
      // Another check after the slow allocation
      if( _newchm != null )     // See if resize is already in progress
        return _newchm;         // Use the new table already

      // The new table must be CAS'd in so only 1 winner amongst duplicate
      // racing resizing threads.  Extra CHM's will be GC'd.
      if( CAS_newchm( newchm ) ) { // NOW a resize-is-in-progress!
        //notifyAll();            // Wake up any sleepers
        //long nano = System.nanoTime();
        //System.out.println(" "+nano+" Resize from "+oldlen+" to "+(1<<log2)+" and had "+(_resizers-1)+" extras" );
        //System.out.print("["+log2);
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
    static private final AtomicLongFieldUpdater<CHM> _copyIdxUpdater =
      AtomicLongFieldUpdater.newUpdater(CHM.class, "_copyIdx");

    // Work-done reporting.  Used to efficiently signal when we can move to
    // the new table.  From 0 to len(oldkvs) refers to copying from the old
    // table to the new.
    volatile long _copyDone= 0;
    static private final AtomicLongFieldUpdater<CHM> _copyDoneUpdater =
      AtomicLongFieldUpdater.newUpdater(CHM.class, "_copyDone");

    // --- help_copy_impl ----------------------------------------------------
    // Help along an existing resize operation.  We hope its the top-level
    // copy (it was when we started) but this CHM might have been promoted out
    // of the top position. 
    private final void help_copy_impl( final boolean copy_all ) {
      final CHM newchm = _newchm;
      assert newchm != null;    // Already checked by caller
      int oldlen = _keys.length; // Total amount to copy
      final int MIN_COPY_WORK = Math.min(oldlen,8); // Limit per-thread work

      // ---
      int panic_start = -1;
      int copyidx=-9999;            // Fool javac to think it's initialized
      while( _copyDone < oldlen ) { // Still needing to copy?
        // Carve out a chunk of work.  The counter wraps around so every
        // thread eventually tries to copy every slot repeatedly.

        // We "panic" if we have tried TWICE to copy every slot - and it still
        // has not happened.  i.e., twice some thread somewhere claimed they
        // would copy 'slot X' (by bumping _copyIdx) but they never claimed to
        // have finished (by bumping _copyDone).  Our choices become limited:
        // we can wait for the work-claimers to finish (and become a blocking
        // algorithm) or do the copy work ourselves.  Tiny tables with huge
        // thread counts trying to copy the table often 'panic'.
        if( panic_start == -1 ) { // No panic?
          copyidx = (int)_copyIdx;
          while( copyidx < (oldlen<<1) && // 'panic' check
                 !_copyIdxUpdater.compareAndSet(this,copyidx,copyidx+MIN_COPY_WORK) )
            copyidx = (int)_copyIdx;     // Re-read
          if( !(copyidx < (oldlen<<1)) ) // Panic!
            panic_start = copyidx;       // Record where we started to panic-copy
        }
      
        // We now know what to copy.  Try to copy.
        int workdone = 0;
        for( int i=0; i<MIN_COPY_WORK; i++ )
          if( copy_slot((copyidx+i)&(oldlen-1)) ) // Made an oldtable slot go dead?
            workdone++;         // Yes!
        if( workdone > 0 )      // Report work-done occasionally
          copy_check_and_promote( workdone );// See if we can promote
        //for( int i=0; i<MIN_COPY_WORK; i++ )
        //  if( copy_slot((copyidx+i)&(oldlen-1)) ) // Made an oldtable slot go dead?
        //    copy_check_and_promote( 1 );// See if we can promote

        copyidx += MIN_COPY_WORK;
        // Uncomment these next 2 lines to turn on incremental table-copy.
        // Otherwise this thread continues to copy until it is all done.
        if( !copy_all && panic_start == -1 ) // No panic?
          return;               // Then done copying after doing MIN_COPY_WORK
      }
      // Extra promotion check, in case another thread finished all copying
      // then got stalled before promoting.
      copy_check_and_promote( 0 ); // See if we can promote
    }
....
    
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
        if( _nbhml.CAS(_chm_offset,this,_newchm) ) { // Promote!
          //System.out.println(" "+nano+" Promote table to "+len(_newkvs));
          System.out.print("]");
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
          System.out.println(""+nano+" copy oldslot="+idx+"/"+_keys.length+" K="+K+" no_slot="+cnt+"/"+len+" slots="+slots+" live="+_nbhml.size()+"");
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
    public SnapshotV(CHM chm) { 
      // Force in-progress table resizes to complete
      CHM topchm = chm._nbhml._chm;
      while( topchm._newchm != null) {
        topchm.help_copy_impl(true);
        topchm = topchm._nbhml._chm;
      }
      _chm = topchm;
      _idx = -2; 
      next(); 
    }
    int length() { return _chm._keys.length; }
    long key(final int idx) { return _chm._keys[idx]; }
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
      _chm._nbhml.remove(_prevK);
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
    public TypeV setValue(final TypeV val) {
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
      public boolean remove( final Object o ) {
        if (!(o instanceof Map.Entry)) return false;
        final Map.Entry<?,?> e = (Map.Entry<?,?>)o;
        return NonBlockingHashMapLong.this.remove(e.getKey(), e.getValue());
      }
      public boolean contains(final Object o) {
        if (!(o instanceof Map.Entry)) return false;
        final Map.Entry<?,?> e = (Map.Entry<?,?>)o;
        TypeV v = get(e.getKey());
        return v.equals(e.getValue());
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
