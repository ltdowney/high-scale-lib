/*
 * Written by Cliff Click and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

package org.cliffc.high_scale_lib;
import java.util.*;
import java.io.Serializable;

// A Non-Blocking Set of primitive 'int'.
// Requires: maximum element on construction

// General note of caution: The Set API allows the use of 'Integer' with
// silent autoboxing - which can be very expensive if many calls are being
// made.  Since autoboxing is silent you may not be aware that this is going
// on.  The built-in API takes lower-case ints and is more efficient.

// Space: space is used in proportion to the largest element, as opposed to
// the number of elements (as is the case with hash-table based Set
// implementations).  Space is approximately (largest_element/8 + 64) bytes.

// The implementation is a simple bit-vector using CAS for update.
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import sun.misc.Unsafe;

public class NonBlockingSetInt extends AbstractSet<Integer> implements Serializable {

  // --- Bits to allow Unsafe access to arrays
  private static final Unsafe _unsafe = UtilUnsafe.getUnsafe();
  private static final int _Lbase  = _unsafe.arrayBaseOffset(long[].class);
  private static final int _Lscale = _unsafe.arrayIndexScale(long[].class);
  private static long rawIndex(final long[] ary, final int idx) {
    assert idx >= 0 && idx < ary.length;
    return _Lbase + idx * _Lscale;
  }
  private final boolean CAS( int idx, long old, long nnn ) {
    return _unsafe.compareAndSwapLong( _bits, rawIndex(_bits, idx), old, nnn );
  }

  // Used to count elements: a high-performance counter.
  private transient final ConcurrentAutoTable _size;

  // The Bits
  private final long _bits[];
  private final boolean in_range( int idx ) {
    return idx >=0 && (idx>>>6) < _bits.length;
  }
  private final long mask( int i ) { return 1L<<(i&63); }

  // I need 1 free bit out of 64 to allow for resize.  I do this by stealing
  // the high order bit - but then I need to do something with adding element
  // number 63 (and friends).  I could use a mod63 function but it's more
  // efficient to handle the mod-64 case as an exception.
  //
  // Every 64th bit is put in it's own recursive bitvector.  If the low 6 bits
  // are all set, we shift them off and recursively operate on the _nbsi64 set.
  private final NonBlockingSetInt _nbsi64;

  private NonBlockingSetInt( int max_elem ) { 
    super(); 
    _bits = new long[(int)(((long)max_elem+63)>>>6)];
    _size = new ConcurrentAutoTable();
    _nbsi64 = ((max_elem+1)>>>6) == 0 ? null : new NonBlockingSetInt((max_elem+1)>>>6);
  }

  public NonBlockingSetInt( ) { 
    this(63);
  }

  // Lower-case 'int' versions - no autoboxing, very fast.
  // Negative values are not allowed.
  public boolean add ( final int i ) {
    if( !in_range(i) ) throw new IllegalArgumentException(""+i);
    if( (i&63) == 63 ) return _nbsi64.add(i>>>6);
    final long mask = mask(i);
    long old;
    do { 
      old = _bits[i>>>6]; // Read old bits.
      if( (old & mask) != 0 ) return false; // Bit is already set?
    } while( !CAS( i>>>6, old, old | mask ) );
    _size.add(1);
    return true;
  }
  public boolean remove  ( final int i ) {
    if( !in_range(i) ) return false;
    if( (i&63) == 63 ) return _nbsi64.remove(i>>>6);
    final long mask = mask(i);
    long old;
    do { 
      old = _bits[i>>6]; // Read old bits
      if( (old & mask) == 0 ) return false; // Bit is already clear?
    } while( !CAS( i>>>6, old, old & ~mask ) );
    _size.add(-1);
    return true;
  }

  public boolean contains( final int i ) { 
    if( !in_range(i) ) return false;
    if( (i&63) == 63 ) return _nbsi64.contains(i>>>6);
    return (_bits[i>>>6] & mask(i)) != 0;
  }

  // Versions compatible with 'Integer' and AbstractSet
  public boolean add ( final Integer o ) { 
    return add(o.intValue()); 
  }
  public boolean contains( final Object  o ) { 
    return o instanceof Integer ? contains(((Integer)o).intValue()) : false; 
  }
  public boolean remove( final Object  o ) { 
    return o instanceof Integer ? remove  (((Integer)o).intValue()) : false; 
  }
  public int size() { 
    return (int)_size.sum() + (_nbsi64==null ? 0 : _nbsi64.size()); 
  }
  public void clear( ) { 
    for( int i=0; i<_bits.length; i++ ) {
      long old = _bits[i>>>6];
      if( old != 0L ) {
        while( !CAS( i>>>6, old, 0L ) )
          old = _bits[i>>>6];
        _size.add(-Long.bitCount(old));
      }
      if( _nbsi64 != null ) _nbsi64.clear();
    }
  }

  // Standard Java iterator.  Not terribly efficient.
  // A faster iterator is: 
  //   for( int i=0; i<nbsi.maxint; i++ )
  //     if( nbsi.contains(i) )
  //       ...i...
  public Iterator<Integer> iterator(       ) { return new iter(); }
  
  private class iter implements Iterator<Integer> {
    int _idx  = -1;
    int _prev = -1;
    iter() { advance(); }
    public boolean hasNext() { return _idx != -2; }
    private void advance() {
      while( true ) {
        _idx++;
        if( !in_range(_idx) ) { _idx = -2; return; }
        if( contains(_idx) ) return;
      }
    }
    public Integer next() { 
      if( _idx == -1 ) throw new NoSuchElementException();
      _prev = _idx;
      advance();
      return _prev;
    }
    public void remove() { 
      if( _prev == -1 ) throw new IllegalStateException();
      NonBlockingSetInt.this.remove(_prev);
      _prev = -1;
    }
  }
}
