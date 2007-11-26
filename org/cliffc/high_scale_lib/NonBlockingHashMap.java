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

public class NonBlockingHashMap<TypeK, TypeV> 
  extends AbstractMap<TypeK,TypeV> 
  implements ConcurrentMap<TypeK, TypeV>, Serializable {

  private static final long serialVersionUID = 1234123412341234123L;

  private static final int REPROBE_LIMIT=10; // Too many reprobes then force a table-resize

  // --- Bits to allow Unsafe access to arrays
  private static final Unsafe _unsafe = UtilUnsafe.getUnsafe();
  private static final int _Obase  = _unsafe.arrayBaseOffset(Object[].class);
  private static final int _Oscale = _unsafe.arrayIndexScale(Object[].class);
  private static long rawIndex(Object[] ary, int i) {
    assert i >= 0 && i < ary.length;
    return _Obase + i * _Oscale;
  }

  // Setup to use Unsafe
  private static final long _kvs_offset;
  static {                      // <clinit>
    Field f = null;
    try { 
      f = NonBlockingHashMap.class.getDeclaredField("_kvs"); 
    } catch( java.lang.NoSuchFieldException e ) {
    } 
    _kvs_offset = _unsafe.objectFieldOffset(f);
  }
  private final boolean CAS_kvs( Object[] oldkvs, Object[] newkvs ) {
    return _unsafe.compareAndSwapObject(this, _kvs_offset, oldkvs, newkvs );
  }

  // A simple boxing and unboxing scheme - a way to 'mark' any Value without
  // hiding the actual value.
  public static final class Prime {
    public Object _V;
    Prime( Object V ) { _V = V; }
    static Object unbox( Object V ) { return V instanceof Prime ? ((Prime)V)._V : V;  }
  }


  // --- The Hash Table --------------------
  // Slot 0 is always used for a 'CHM' entry below to hold the interesting
  // bits of the hash table.  Slot 1 holds full hashes as an array of ints.
  // Slots {2,3}, {4,5}, etc hold {Key,Value} pairs.  The entire hash table
  // can be atomically replaced by CASing the _kvs field.
  //
  // Why is CHM buried inside the _kvs Object array, instead of the other way
  // around?  The CHM info is used during resize events and updates, but not
  // during standard 'get' operations.  I assume 'get' is much more frequent
  // than 'put'.  'get' can skip the extra indirection of skipping through the
  // CHM to reach the _kvs array.
  private transient Object[] _kvs;
  private static final CHM   chm   (Object[] kvs) { return (CHM  )kvs[0]; }
  private static final int[] hashes(Object[] kvs) { return (int[])kvs[1]; }
  // Number of K,V pairs in the table
  public static final int len(Object[] kvs) { return (kvs.length-2)>>1; }

  // --- Minimum table size ----------------
  // Pick size 16 K/V pairs, which turns into (16*2+2)*4+12 = 148 bytes on a
  // standard 32-bit HotSpot, and (16*2+2)*8+12 = 284 bytes on 64-bit Azul.
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

  // --- key,val -------------------------------------------------------------
  // Access K,V for a given idx
  //
  // Note that these are static, so that the caller is forced to read the _kvs
  // field only once, and share that read across all key/val calls - lest the
  // _kvs field move out from under us and back-to-back key & val calls refer
  // to different _kvs arrays.
  public static final Object key(Object[] kvs,int idx) { return kvs[(idx<<1)+2]; }
  public static final Object val(Object[] kvs,int idx) { return kvs[(idx<<1)+3]; }
  private static final boolean CAS_key( Object[] kvs, int idx, Object old, Object key ) {
    return _unsafe.compareAndSwapObject( kvs, rawIndex(kvs,(idx<<1)+2), old, key );
  }
  private static final boolean CAS_val( Object[] kvs, int idx, Object old, Object val ) {
    return _unsafe.compareAndSwapObject( kvs, rawIndex(kvs,(idx<<1)+3), old, val );
  }
   

  // --- dump ----------------------------------------------------------------
  public final void dump() { 
    System.out.println("=========");
    dump2(_kvs);
    System.out.println("=========");
  }
  // dump the entire state of the table
  private final void dump( Object[] kvs ) { 
    for( int i=0; i<len(kvs); i++ ) {
      Object K = key(kvs,i);
      if( K != null ) {
        String KS = (K == TOMBSTONE) ? "XXX" : K.toString();
        Object V = val(kvs,i);
        Object U = Prime.unbox(V);
        String p = (V==U) ? "" : "prime_";
        String US = (U == TOMBSTONE) ? "tombstone" : U.toString();
        System.out.println(""+i+" ("+KS+","+p+US+")");
      }
    }
    Object[] newkvs = chm(kvs)._newkvs; // New table, if any
    if( newkvs != null ) {
      System.out.println("----");
      dump(newkvs);
    }
  }
  // dump only the live values, broken down by the table they are in
  private final void dump2( Object[] kvs) { 
    for( int i=0; i<len(kvs); i++ ) {
      Object key = key(kvs,i);
      Object val = val(kvs,i);
      Object U = Prime.unbox(val);
      if( key != null && key != TOMBSTONE &&  // key is sane
          val != null && U   != TOMBSTONE ) { // val is sane
        String p = (val==U) ? "" : "prime_";
        System.out.println(""+i+" ("+key+","+p+val+")");
      }
    }
    Object[] newkvs = chm(kvs)._newkvs; // New table, if any
    if( newkvs != null ) {
      System.out.println("----");
      dump2(newkvs);
    }
  }

  // --- hash ----------------------------------------------------------------
  // Helper function to spread lousy hashCodes
  private static final int hash(Object key) {
    int h = key.hashCode();     // The real hashCode call
    h ^= (h>>>20) ^ (h>>>12);   // Spread bits about
    h ^= (h>>> 7) ^ (h>>> 4);
    return h;
  }

  // --- NonBlockingHashMap --------------------------------------------------
  public NonBlockingHashMap( ) { this(MIN_SIZE); }
  public NonBlockingHashMap( int initial_sz ) { initialize(initial_sz); }
  void initialize(int initial_sz ) { 
    if( initial_sz < 0 ) throw new IllegalArgumentException();
    int i;                      // Convert to next largest power-of-2
    for( i=MIN_SIZE_LOG; (1<<i) < initial_sz; i++ ) ;
    // Double size for K,V pairs, add 1 for CHM and 1 for hashes
    _kvs = new Object[((1<<i)<<1)+2];
    _kvs[0] = new CHM(new ConcurrentAutoTable()); // CHM in slot 0
    _kvs[1] = new int[1<<i];                      // Matching hash entries
  }

  // --- wrappers ------------------------------------------------------------
  public int size() { return chm(_kvs).size(); }
  public boolean containsKey( Object key )            { return get(key) != null; }
  public boolean contains   ( Object val )            { return contains(_kvs,val); }
  public TypeV put          ( TypeK  key, TypeV val ) { return (TypeV)putIfMatch( key,  val, NO_MATCH_OLD );  }
  public TypeV putIfAbsent  ( TypeK  key, TypeV val ) { return (TypeV)putIfMatch( key,  val, null );  }
  public TypeV remove       ( Object key )            { return (TypeV)putIfMatch( key, null, NO_MATCH_OLD );  }
  public boolean remove     ( Object key, Object val ){ return        putIfMatch( key, null, val ) == val; }
  public boolean replace    ( TypeK  key, TypeV  oldValue, TypeV newValue) {
    if (oldValue == null || newValue == null)  throw new NullPointerException();
    return putIfMatch( key, newValue, oldValue ) == oldValue;
  }
  public TypeV replace( TypeK key, TypeV val ) {
    if (val == null)  throw new NullPointerException();
    return putIfAbsent( key, val );
  }
  private final Object putIfMatch( Object key, TypeV val, Object oldVal ) {
    Object newval = val;
    if( newval == null ) newval = TOMBSTONE;
    if( oldVal == null ) oldVal = TOMBSTONE;
    Object res = putIfMatch( _kvs, key, newval, oldVal );
    assert !(res instanceof Prime);
    return res == TOMBSTONE ? null : res;
  }

  public void putAll(Map<? extends TypeK, ? extends TypeV> t) {
    Iterator<? extends Map.Entry<? extends TypeK, ? extends TypeV>> i = t.entrySet().iterator();
    while (i.hasNext()) {
      Map.Entry<? extends TypeK, ? extends TypeV> e = i.next();
      put(e.getKey(), e.getValue());
    }
  }

  // Atomically replace the k/v array with a new empty array
  public void clear() {         // Smack a new empty table down
    Object[] newkvs = new NonBlockingHashMap(MIN_SIZE)._kvs;
    while( !CAS_kvs(_kvs,newkvs) ) // Spin until the clear works
      ;
  }

  // --- contains ------------------------------------------------------------
  // Search for matching value.
  private static final boolean contains( Object[] kvs, Object val ) {
    final int len = len(kvs);   // Count of key/value pairs
    // Simple scan loop - assuming no table-copy in progress
    int i;
    for( i=0; i<len; i++ ) {    // Check the whole values array
      Object V = val(kvs,i);    // Get a value
      if( V == val || val.equals(V) ) // Equals?  Found it!
        return true;            // Return hit
      if( V instanceof Prime )  // Mid-copy?
        break;                  // Oops, need slower scan
    }
    if( i==len ) return false;  // Scanned whole table, must be a miss

    // Slower scan loop; must keep checking for partially copied values
    CHM chm = chm(kvs);         // 
    Object[] newkvs = chm._newkvs; // Read the volatile only once
    int copy_cnt = 0;           // Count of copied slots
    for( ; i<len; i++ ) {       // Check the rest of the values array
      Object V = val(kvs,i);    // Get a value
      if( V == val || val.equals(V) ) // Equals?  Found it!
        break;                  // Stop scanning, i<len indicates 'found'
      if( V instanceof Prime ) {// Mid-copy?
        // Mid-copy!  Force this slot to copy to the new table so that when we
        // recursively scan the new table we'll find the updated value there.
        // Note that is it incorrect to merely test the value in the box - as
        // it might have been overridden in the new table.  The only thing we
        // can do with a boxed value is copy it to the new table.
        if( chm.copy_slot(i,kvs,newkvs) ) // Force this slot to copy
          copy_cnt++;           // And count if this thread did the copy
      }
      // Periodically roll up any copy-counts and check for promotion
      if( copy_cnt > 0 && (len&63)==63 ) {
        chm.copy_check_and_promote(copy_cnt); 
        copy_cnt=0;
      }
    }

    if( copy_cnt > 0 )      // Roll up any copy-counts
      chm.copy_check_and_promote(copy_cnt); 
    if( i < len ) return true;  // Found it!
    return contains(newkvs,val);// Not found in this table, so scan in next
  }

  // --- keyeq ---------------------------------------------------------------
  // Check for key equality.  Try direct pointer compare first, then see if
  // the hashes are unequal (fast negative test) and finally do the full-on
  // 'equals' v-call.
  private static boolean keyeq( Object K, Object key, int[] hashes, int hash, int fullhash ) {
    return 
      K==key ||             // Either keys match exactly OR
      // hash exists and matches?
      ((hashes[hash] == 0 || hashes[hash] == fullhash) &&
       key.equals(K));          // Finally do the hard match
  }

  // --- get -----------------------------------------------------------------
  // Get!  Returns 'null' to mean Tombstone or empty.  
  // Never returns a Prime nor a Tombstone.
  public final TypeV get( Object key ) {
    Object V = get_impl(_kvs,key);
    assert !(V instanceof Prime); // Never return a Prime
    return (TypeV)V;
  }

  private static final Object get_impl( Object[] kvs, Object key ) {
    final int fullhash = hash(key); // throws NullPointerException if key is null
    final int   len    = len   (kvs); // Count of key/value pairs
    CHM chm = chm(kvs);               // The CHM, for a volatile read below; reads slot 0 of kvs
    final int[] hashes = hashes(kvs); // The memoized hashes; reads slot 1 of kvs

    int idx = fullhash & (len-1); // First key hash

    // Main spin/reprobe loop, looking for a Key hit
    int reprobe_cnt=0;
    while( true ) {
      // Probe table
      final Object V = val(kvs,idx); // Get value before volatile read, could be Tombstone/empty or sentinel
      final Object K = key(kvs,idx); // First key
      if( K == null ) return null;   // A clear miss

      // We need a volatile-read here to preserve happens-before semantics on
      // newly inserted Keys.  If the Key body was written just before inserting
      // into the table a Key-compare here might read the uninitalized Key body.
      // Annoyingly this means we have to volatile-read before EACH key compare.
      // .
      // We also need a volatile-read between reading a newly inserted Value
      // and returning the Value (so the user might end up reading the stale
      // Value contents).  Same problem as with keys - and the one volatile
      // read covers both.
      Object[] dummy = chm._newkvs; // VOLATILE READ before key compare
      
      // Key-compare
      if( keyeq(K,key,hashes,idx,fullhash) ) {
        // Key hit!  Check for no table-copy-in-progress
        if( !(V instanceof Prime) ) // No copy?
          return (V == TOMBSTONE) ? null : V; // Return the value
        // Key hit - but slot is copied.  Finish the copy & retry in the new table.  
        Object[] newkvs = chm.copy_slot_and_check(idx,kvs); // Force this slot to copy
        help_copy();                 // Help along any partial copy
        return get_impl(newkvs,key); // Retry in the new table
      }
      // get and put must have the same key lookup logic!  But only 'put'
      // needs to force a table-resize for a too-long key-reprobe sequence.
      // Check for too-many-reprobes on get.
      if( ++reprobe_cnt >= (REPROBE_LIMIT + (len>>2)) )
        return null;            // This is treated as a MISS in this table.
      idx = (idx+1)&(len-1);    // Reprobe by 1!  (should force a prefetch)
    }
  }
  
  // --- putIfMatch ---------------------------------------------------------
  // Put, Remove, PutIfAbsent, etc.  Return the old value.  If the returned
  // value is equal to oldVal (or oldVal is NO_MATCH_OLD) then the put can be
  // assumed to work (although might have been immediately overwritten).
  private static final Object putIfMatch( Object[] kvs, Object key, Object putval, Object expVal ) {
    assert !(putval instanceof Prime);
    assert !(expVal instanceof Prime);
    assert putval != null;
    assert expVal != null;
    CHM chm = chm(kvs);
    final int   len    = len   (kvs); // Count of key/value pairs
    final int[] hashes = hashes(kvs);
    final int fullhash = hash(key); // throws NullPointerException if key null
    int hash = fullhash & (len-1);

    // ---
    // Key-Claim stanza: spin till we can claim a Key (or force a resizing).
    boolean assert_goto_new_table = false;
    int reprobe_cnt=0;
    Object K=null,V=null;
    while( true ) {             // Spin till we get it
      V = val(kvs,hash);        // Get old value (before volatile read below!)
      K = key(kvs,hash);        // Get current key
      //if( ((hash<<1)&(4-1))== 0 )
      //  Prefetch.shared(kvs,_Obase+((hash<<1)+2)*_Oscale+128);
      if( K == null ) {         // Slot is free?
        if( putval == TOMBSTONE ) return null; // Not-now & never-been in this table
        // If the table is getting full I do not want to install a new key in
        // this old table - instead end all key chains and fall into the next
        // code, which will move on to the new table.
        if( chm.tableFull(reprobe_cnt,len) && // Table is full?
            CAS_key(kvs,hash,null,CHECK_NEW_TABLE_SENTINEL) ) {
          assert_goto_new_table = true; // for asserts only
          chm.copy_done(kvs,1,this); // nuked an old-table slot
        }
        if( CAS_key(kvs,hash, null, key ) ) { // Claim slot for Key
          chm._slots.add(1);    // Raise slot count
          if( hashes != null ) hashes[hash] = fullhash; // Memoize fullhash 
          break;                // Got it!
        }
        K = key(kvs,hash);      // CAS failed, get updated value
        assert K != null ;      // If keys[hash] is null, CAS shoulda worked
      }
      Object[] dummy = chm._newkvs; // VOLATILE READ before key compare
      if( keyeq(K,key,hashes,hash,fullhash) )
        break;                  // Got it!
      
      // get and put must have the same key lookup logic!  Lest 'get' give
      // up looking too soon.  
      if( K == CHECK_NEW_TABLE_SENTINEL ||
          ++reprobe_cnt >= (REPROBE_LIMIT + (len>>2)) ) {
        // We simply must have a new table to do a 'put'.  At this point a
        // 'get' will also go to the new table (if any).  We do not need
        // to claim a key slot (indeed, we cannot find a free one to claim!).
        help_copy();
        return putIfMatch(chm.resize(kvs,this),key,putval,expVal);
      }      
      hash = (hash+1)&(len-1); // Reprobe!
    }
    

    // ---
    // Found the proper Key slot, now update the matching Value slot
    if( putval == V ) return V; // Fast cutout for no-change

    // See if we want to move to a new table
    // It is OK to force a new table "early", because the put in the new
    // table will override anything in the old table - so any get following
    // this put will see the sentinel and move to the new table.
    if( (chm._newkvs != null || // Table copy already in progress?
         chm.tableFull(reprobe_cnt,len)) ) { // Or table is full?

      // New table is forced if table is full & not resize already in progress
      Object[] newkvs = chm.resize(kvs,this);
      
      // Copy the old table to the new table.  This copy can CAS spin,
      // however, the number of times the CAS can fail is bounded by the
      // number of late-arriving 'put' operations who don't realize a table
      // copy is in progress (i.e., limited by thread count).
      chm.copy_one_done(hash,kvs,newkvs,this);
      help_copy();
      // Now put into the new table
      return putIfMatch(newkvs,key,putval,expVal);
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
    if( CAS_val(kvs, hash, V, putval ) ) { // Note: no looping on this CAS failing
      // CAS succeeded - we did the update!
      // Adjust sizes - a striped counter
      if(  (V == null || V == TOMBSTONE) && putval != TOMBSTONE ) chm._size.add( 1);
      if( !(V == null || V == TOMBSTONE) && putval == TOMBSTONE ) chm._size.add(-1);
    }
    // Win or lose the CAS, we are done.  If we won then we know the update
    // happened as expected.  If we lost, it means "we won but another thread
    // immediately stomped our update with no chance of a reader reading".
    return V; 
  }
    
  // --- help_copy ---------------------------------------------------------
  // Help along an existing resize operation.  This is just a fast cut-out
  // wrapper, to encourage inlining for the fast no-copy-in-progress case.
  private final void help_copy( ) {
    Object[] topkvs = _kvs;        // Read top-level table once
    CHM topchm = chm(topkvs);
    if( topchm._newkvs != null )
      topchm.help_copy_impl(topkvs,this,false);
  }
  

  // --- CHM -----------------------------------------------------------------
  private static final class CHM<TypeK,TypeV> {
    // Size in active K,V pairs
    private final ConcurrentAutoTable _size;
    public int size () { return (int)_size.sum(); }

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
    // Time since last resize
    private long _last_resize_milli;
    
    // New mappings, used during resizing.
    // The 'next' CHM - created during a resize operation.  This represents
    // the new table being copied from the old one.  It monotonically transits
    // from null to set (once).
    volatile Object[] _newkvs;
    private final AtomicReferenceFieldUpdater<CHM,Object[]> _newkvsUpdater =
      AtomicReferenceFieldUpdater.newUpdater(CHM.class,Object[].class, "_newkvs");
    // Set the _next field if we can.
    boolean CAS_newkvs( Object[] newkvs ) { 
      while( _newkvs == null ) 
        if( _newkvsUpdater.compareAndSet(this,null,newkvs) )
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
    static private final AtomicLongFieldUpdater<CHM> _resizerUpdater =
      AtomicLongFieldUpdater.newUpdater(CHM.class, "_resizers");

    // Simple constructor
    CHM( ConcurrentAutoTable size ) {
      _size = size;
      _slots= new ConcurrentAutoTable();
      _last_resize_milli = System.currentTimeMillis();
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
    private final Object[] resize(Object[] kvs, NonBlockingHashMap topmap) {
      assert chm(kvs) == this;

      Object[] newkvs;
      while( true ) {
        // Check for resize already in progress, probably triggered by another thread
        newkvs = _newkvs;
        if( newkvs != null )    // See if resize is already in progress
          return newkvs;        // Use the new table already
        // If we are a nested table, force the top-level table to finish resizing
        Object[] topkvs = topmap._kvs; // Read top-level table once
        CHM topchm = chm(topkvs);
        if( topchm == this )  break;
        topchm.help_copy_impl(topkvs,topmap,true);
      }
      
      int oldlen = len(kvs);    // Old count of K,V pairs allowed
      assert slots() >= REPROBE_LIMIT+(oldlen>>2); // No change in size needed?
      int sz = topmap.size();   // Get current table count of active K,V pairs
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
        newkvs = _newkvs;
        if( newkvs != null )    // See if resize is already in progress
          return newkvs;        // Use the new table already
        // TODO - use a wait with timeout, so we'll wakeup as soon as the new table
        // is ready, or after the timeout in any case.
        //synchronized( this ) { wait(8*megs); }         // Timeout - we always wakeup
        try { Thread.sleep(8*megs); } catch( Exception e ) { }
      }
      // Last check, since the 'new' below is expensive and there is a chance
      // that another thread slipped in a new thread while we ran the heuristic.
      newkvs = _newkvs;
      if( newkvs != null )      // See if resize is already in progress
        return newkvs;          // Use the new table already

      // Double size for K,V pairs, add 1 for CHM
      newkvs = new Object[((1<<log2)<<1)+2]; // This can get expensive for big arrays
      newkvs[0] = new CHM(_size); // CHM in slot 0
      if( kvs[1] != null ) newkvs[1] = new int[1<<log2];
      
      // Another check after the slow allocation
      if( _newkvs != null )     // See if resize is already in progress
        return _newkvs;         // Use the new table already

      // The new table must be CAS'd in so only 1 winner amongst duplicate
      // racing resizing threads.  Extra CHM's will be GC'd.
      //long nano = System.nanoTime();
      if( CAS_newkvs( newkvs ) ) { // Now a resize-is-in-progress!
        //notifyAll();            // Wake up any sleepers
        //System.out.println(" "+nano+" Resize from "+oldlen+" to "+(1<<log2)+" and had "+(_resizers-1)+" extras" );
      } else                    // CAS failed?
        newkvs = _newkvs;       // Reread new table
      return newkvs;
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
    // Help along an existing resize operation.
    private final void help_copy_impl( Object[] oldkvs, NonBlockingHashMap topmap, boolean copy_all ) {
      int oldlen = (int)len (oldkvs);
      final int MIN_COPY_WORK = Math.min(oldlen,64);
      Object[] newkvs = _newkvs;

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
          if( copy_one((copyidx+i)&(oldlen-1),oldkvs,newkvs,topmap) ) // Made an oldtable slot go dead?
            workdone++;         // Yes!
            
        if( workdone > 0 ) {    // Report work-done occasionally
          copy_done( oldkvs, workdone, topmap );
          if( panicidx == -1 && !copy_all ) return; // If not forcing copy-all, then get out!  We did our share of copy-work
        }
        copyidx += MIN_COPY_WORK;
        if( panicidx+oldlen <= copyidx )
          break;                // Panic-copied the whole array?
      }
      
      //if( panicidx != -1 && copyidx-panicidx > 100 )
      //  System.out.println("Panic copy from "+panicidx+" to "+copyidx);
    }

    
    // --- copy_slot_and_check -----------------------------------------------
    // Copy slot 'i' from the old table to the new table.  If this thread
    // confirmed the copy, update the counters and check for promotion.
    private final void copy_slot_and_check( int idx, Object[] oldkvs ) {
      if( copy_one(idx,oldkvs,_newkvs) )
        copy_check_and_promote(1);
    }

    // --- copy_done ---------------------------------------------------------
    private final void copy_check_and_promote( int workdone ) {
      // We made a slot unusable and so did some of the needed copy work
      long copyDone = _copyDone;
      while( !_copyDoneUpdater.compareAndSet(this,copyDone,copyDone+workdone) ) 
        copyDone = _copyDone;   // Reload, retry
      if( copyDone+workdone > len(oldkvs) )
        throw new Error("too much copyDone:"+copyDone+" work="+workdone+" > len="+len(oldkvs));
      assert (copyDone+workdone) <= len(oldkvs);
      // Check for copy being ALL done, and promote
      if( copyDone+workdone == len(oldkvs) ) {
        //long nano = System.nanoTime();
        if( topmap.CAS_kvs(oldkvs,_newkvs) ) {
          //System.out.println(" "+nano+" Promote table to "+len(_newkvs));
        }
      }
    }    

    // --- copy_slot ---------------------------------------------------------
    // Copy one K/V pair from oldkvs[i] to newkvs.  Returns true if we can
    // confirm that the new table guaranteed has a value for this old-table
    // slot.  We need an accurate confirmed-copy count so that we know when we
    // can promote (if we promote the new table too soon, other threads may
    // 'miss' on values not-yet-copied from the old table).  We don't allow
    // any direct updates on the new table, unless they first happened to the
    // old table - so that any transition in the new table from null to
    // not-null must have been from a copy_slot (or other old-table overwrite)
    // and not from a thread directly writing in the new table.  Thus we can
    // count null-to-not-null transitions in the new table.
    private static boolean copy_slot( int idx, Object[] oldkvs, Object[] newkvs ) {
      if( true ) {
        throw new Unimplemented();
      } else {
      Object key = key(oldkvs,idx);
      if( key == CHECK_NEW_TABLE_SENTINEL ) // slot already dead?
        return false;           // Slot dead but we did not do it
      if( key == null ) {         // Try to kill a dead slot
        if( CAS_key(oldkvs,idx, null, CHECK_NEW_TABLE_SENTINEL ) )
          return true;          // We made slot go dead
        // CAS from null-2-CHECK_NEW failed.  Check for slot already dead
        key = key(oldkvs,idx);      // Reload after failed CAS
        assert key != null;
        if( key == CHECK_NEW_TABLE_SENTINEL ) // slot already dead?
          return false;         // Slot dead but we did not do it
      }
      final int   len    = len   (newkvs); // Count of key/value pairs
      // ought to copy hash here, no call hash() again
      final int[] hashes = hashes(newkvs);
      final int fullhash = (hashes != null) ? hashes(oldkvs)[idx] : hash(key);
      int hash = fullhash & (len-1);
      CHM newchm = chm(newkvs);

      // ---
      // Key-Claim stanza: spin till we can claim a Key (or force a resizing).
      int cnt=0;
      while( true ) {                // Spin till we get key slot
        Object K = key(newkvs,hash); // Get current key
        if( K == null ) {            // Slot is free?
          Object V = val(oldkvs,idx);  // Read OLD table
          assert !(V instanceof Prime);
          if( V == CHECK_NEW_TABLE_SENTINEL ) return false; // Dead in old, not in new, so copy complete
          // Not in old table, not in new table, and no need for it in new table.
          if( V == null || V == TOMBSTONE ) { 
            if( CAS_val(oldkvs, idx, null, CHECK_NEW_TABLE_SENTINEL ) ) // Try to wipe it out now.
              return true;
          }
          // Claim new-table slot for key!
          if( CAS_key(newkvs,hash, null, key ) ) { // Claim slot for Key
            newchm._slots.add(1); // Raise slot count
            if( hashes != null ) hashes[hash] = fullhash; // Memoize fullhash 
            break;                // Got it!
          }
          K = key(newkvs,hash);      // CAS failed, get updated value
          assert K != null ;      // If keys[hash] is null, CAS shoulda worked
        }
        if( keyeq(K,key,hashes,hash,fullhash) )
          break;                  // Got it!

        if( ++cnt >= (REPROBE_LIMIT+(len>>2)) ) {
          Object V = val(oldkvs,idx);  // Read OLD table
          if( V == CHECK_NEW_TABLE_SENTINEL ) return false; // Dead in old, not in new, so copy complete
          // Still in old table, but no space in new???
          long nano = System.nanoTime();
          long slots= newchm.slots();
          long size = size();
          System.out.println(""+nano+" copy oldslot="+idx+"/"+len(oldkvs)+" K="+K+" no_slot="+cnt+"/"+len+" slots="+slots+" live="+size+"");
          throw new Error();
        }
        hash = (hash+1)&(len-1); // Reprobe!
      }
      // We have a Key-slot in the new table now, most likely a {Key,null} pair

      // ---
      // Spin until we complete the copy
      Object newV, oldV;
      boolean did_work = false;
      while( true ) {
        newV = val(newkvs,hash);
        Object[] dummy = _newkvs; // dummy volatile read
        oldV = val(oldkvs,idx);
        assert !(oldV instanceof Prime);
        assert oldV != null;

        // Is our work done here?  Old slot is smacked so no more copy?
        if( oldV == CHECK_NEW_TABLE_SENTINEL )
          break;

        // Check for a mismatch between old and new.  Update the new table
        // with a Primed version of the old table value.
        if( (newV instanceof Prime && ((Prime)newV)._V != oldV) || newV == null ) {
          Prime old_primeV = oldV == TOMBSTONE ? TOMBPRIME : new Prime(oldV);
          if( !CAS_val(newkvs, hash, newV, old_primeV ) )
            continue;           // Failed CAS?  Try to copy again
          newV = old_primeV;    // CAS worked, so newV is a prime copy of oldV
        }
        // newV is now a prime version of oldV, or perhaps newV is not prime at all
        assert( !(newV instanceof Prime) || ((Prime)newV)._V == oldV );
        // Complete copy by killing old slot with a CHECK_NEW
        if( CAS_val(oldkvs, idx, oldV, CHECK_NEW_TABLE_SENTINEL ) ) {
          did_work = true;
          break;
        }
      } // end spin-until-copy-completes

      // Now clear out the Prime bit from the new table
      if( newV instanceof Prime ) {
        if( !CAS_val( newkvs, hash, newV, ((Prime)newV)._V ) ) 
          assert( !(val(newkvs,hash) instanceof Prime) );
      }
      return did_work;
      }
    } // end copy_slot
    
  }

  // --- Snapshot ------------------------------------------------------------
  class SnapshotV implements Iterator<TypeV> {
    final Object[] _kvs;
    public SnapshotV(Object[] kvs) { _kvs = kvs; next(); }
    int length() { return len(_kvs); }
    Object key(int idx) { return NonBlockingHashMap.key(_kvs,idx); }
    private int _idx;           // 0-keys.length
    private Object _nextK, _prevK; // Last 2 keys found
    private TypeV  _nextV, _prevV; // Last 2 values found
    public boolean hasNext() { return _nextV != null; }
    public TypeV next() {
      // 'next' actually knows what the next value will be - it had to
      // figure that out last go-around lest 'hasNext' report true and
      // some other thread deleted the last value.  Instead, 'next'
      // spends all its effort finding the key that comes after the
      // 'next' key.
      if( _idx != 0 && _nextV == null ) throw new NoSuchElementException();
      _prevK = _nextK;          // This will become the previous key
      _prevV = _nextV;          // This will become the previous value
      _nextV = null;            // We have no more next-key
      // Attempt to set <_nextK,_nextV> to the next K,V pair.
      // _nextV is the trigger: stop searching when it is != null
      while( _idx<length() ) {  // Scan array
        _nextK = key(_idx++); // Get a key that definitely is in the set (for the moment!)
        if( _nextK != null && // Found something?
            _nextK != CHECK_NEW_TABLE_SENTINEL &&
            (_nextV=get(_nextK)) != null )
          break;                // Got it!  _nextK is a valid Key
      }                         // Else keep scanning
      return _prevV;            // Return current value.
    }
    public void remove() { 
      if( _prevV == null ) throw new IllegalStateException();
      putIfMatch( _kvs, _prevK, null, _prevV );
      _prevV = null;
    }
  }

  // --- values --------------------------------------------------------------
  public Collection<TypeV> values() {
    return new AbstractCollection<TypeV>() {
      public void    clear   (          ) {        NonBlockingHashMap.this.clear   ( ); }
      public int     size    (          ) { return NonBlockingHashMap.this.size    ( ); }
      public boolean contains( Object v ) { return NonBlockingHashMap.this.containsValue(v); }
      public Iterator<TypeV> iterator()   { return new SnapshotV(_kvs); }
    };
  }

  // --- keySet --------------------------------------------------------------
  class SnapshotK implements Iterator<TypeK> {
    final SnapshotV _ss;
    public SnapshotK(Object[] kvs) { _ss = new SnapshotV(kvs); }
    public void remove() { _ss.remove(); }
    public TypeK next() { _ss.next(); return (TypeK)_ss._prevK; }
    public boolean hasNext() { return _ss.hasNext(); }
  }
  public Set<TypeK> keySet() {
    return new AbstractSet<TypeK> () {
      public void    clear   (          ) {        NonBlockingHashMap.this.clear   ( ); }
      public int     size    (          ) { return NonBlockingHashMap.this.size    ( ); }
      public boolean contains( Object k ) { return NonBlockingHashMap.this.containsKey(k); }
      public boolean remove  ( Object k ) { return NonBlockingHashMap.this.remove  (k) != null; }
      public Iterator<TypeK> iterator()   { return new SnapshotK(_kvs); }
    };
  }

  // --- entrySet ------------------------------------------------------------
  // Warning: Each call to 'next' in this iterator constructs a new WriteThroughEntry.
  class NBHMEntry extends AbstractEntry<TypeK,TypeV> {
    NBHMEntry( final TypeK k, final TypeV v ) { super(k,v); }
    public TypeV setValue(TypeV val) {
      if (val == null) throw new NullPointerException();
      _val = val;
      return put(_key, val);
    }
  }
  class SnapshotE implements Iterator<Map.Entry<TypeK,TypeV>> {
    final SnapshotV _ss;
    public SnapshotE(Object[] kvs) { _ss = new SnapshotV(kvs); }
    public void remove() { _ss.remove(); }
    public Map.Entry<TypeK,TypeV> next() { _ss.next(); return new NBHMEntry((TypeK)_ss._prevK,_ss._prevV); }
    public boolean hasNext() { return _ss.hasNext(); }
  }
  public Set<Map.Entry<TypeK,TypeV>> entrySet() {
    return new AbstractSet<Map.Entry<TypeK,TypeV>>() {
      public void    clear   (          ) {        NonBlockingHashMap.this.clear( ); }
      public int     size    (          ) { return NonBlockingHashMap.this.size ( ); }
      public boolean remove( final Object o ) {
        if (!(o instanceof Map.Entry)) return false;
        Map.Entry<?,?> e = (Map.Entry<?,?>)o;
        return NonBlockingHashMap.this.remove(e.getKey(), e.getValue());
      }
      public boolean contains(final Object o) {
        if (!(o instanceof Map.Entry)) return false;
        Map.Entry<?,?> e = (Map.Entry<?,?>)o;
        TypeV v = get(e.getKey());
        return v.equals(e.getValue());
      }
      public Iterator<Map.Entry<TypeK,TypeV>> iterator() { return new SnapshotE(_kvs); }
    };
  }

  // --- writeObject -------------------------------------------------------
  // Write a NBHM to a stream
  private void writeObject(java.io.ObjectOutputStream s) throws IOException  {
    s.defaultWriteObject();     // Nothing to write
    s.writeBoolean( _kvs[1] != null ); // Memoizing hashes or not
    final Object[] kvs = _kvs;  // The One Field is transient
    for( int i=0; i<len(kvs); i++ ) {
      final Object K = key(kvs,i);
      if( K != null && K != CHECK_NEW_TABLE_SENTINEL ) { // Only serialize keys in this table
        final Object V = get(K); // But do an official 'get' in case key is being copied
        if( V != null ) {     // Key might have been deleted
          s.writeObject(K);   // Write the <TypeK,TypeV> pair
          s.writeObject(V);
        }
      }
    }
    s.writeObject(null);      // Sentinel to indicate end-of-data
    s.writeObject(null);
  }
  
  // --- readObject --------------------------------------------------------
  // Read a CHM from a stream
  private void readObject(java.io.ObjectInputStream s) throws IOException, ClassNotFoundException  {
    s.defaultReadObject();      // Read nothing
    boolean memoize = s.readBoolean();
    initialize(MIN_SIZE,memoize);
    for (;;) {
      final TypeK K = (TypeK) s.readObject();
      final TypeV V = (TypeV) s.readObject();
      if( K == null ) break;
      put(K,V);                 // Insert with an offical put
    }
  }

} // End NonBlockingHashMap class
