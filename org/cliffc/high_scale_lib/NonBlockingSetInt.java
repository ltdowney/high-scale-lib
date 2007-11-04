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
// implementations).  Space is approximately (largest_element/8 + 64bytes).

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

  // Used to count elements
  private transient final ConcurrentAutoTable _size;
  // The Bits
  private final long _bits[];
  private final boolean in_range( int idx ) {
    return idx >=0 && (idx>>6) < _bits.length;
  }
  private final long mask( int i ) { return 1L<<(i&63); }

  public NonBlockingSetInt( int max_elem ) { 
    super(); 
    if( max_elem < 0 ) throw new IllegalArgumentException();
    _bits = new long[(int)(((long)max_elem+63)>>>6)];
    _size = new ConcurrentAutoTable();
  }

  // Lower-case 'int' versions - no autoboxing, very fast
  public boolean add ( final int i ) {
    long mask = mask(i);
    long old;
    do { 
      old = _bits[i>>6]; // Read old bits
      if( (old & mask) != 0 ) return false; // Bit is already set?
    } while( !CAS( i>>6, old, old | mask ) );
    _size.add(1);
    return true;
  }
  public boolean remove  ( final int i ) {
    long mask = mask(i);
    long old;
    do { 
      old = _bits[i>>6]; // Read old bits
      if( (old & mask) == 0 ) return false; // Bit is already clear?
    } while( !CAS( i>>6, old, old & ~mask ) );
    _size.add(-1);
    return true;
  }

  public boolean contains( final int i ) { 
    return in_range(i) && (_bits[i>>6] & mask(i)) != 0;
  }

  // Versions compatible with 'Integer' and AbstractSet
  public boolean add     ( final Integer o ) { return add(o.intValue()); }
  public boolean contains( final Object  o ) { 
    return o instanceof Integer ? contains(((Integer)o).intValue()) : false; }
  public boolean remove  ( final Object  o ) { 
    return o instanceof Integer ? remove  (((Integer)o).intValue()) : false; }
  public int     size    (                 ) { return (int)_size.sum(); }
  public void    clear   (                 ) { 
    for( int i=0; i<_bits.length; i++ ) {
      long old = _bits[i>>6];
      if( old != 0L ) {
        while( !CAS( i>>6, old, 0L ) )
          old = _bits[i>>6];
        _size.add(-Long.bitCount(old));
      }
    }
  }

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
        if( (_bits[_idx>>6] & mask(_idx)) != 0 ) return;
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
