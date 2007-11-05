/*
 * Written by Cliff Click and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

package org.cliffc.high_scale_lib;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.*;
import java.util.*;

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
  private static final Unsafe _unsafe = UtilUnsafe.getUnsafe();

  // --- Bits to allow atomic update of the NBSI
  private static final long _nbsi_offset;
  static {                      // <clinit>
    Field f = null;
    try { 
      f = NonBlockingSetInt.class.getDeclaredField("_nbsi"); 
    } catch( java.lang.NoSuchFieldException e ) {
    } 
    _nbsi_offset = _unsafe.objectFieldOffset(f);
  }
  private final boolean CAS_nbsi( NBSI old, NBSI nnn ) {
    return _unsafe.compareAndSwapObject(this, _nbsi_offset, old, nnn );
  }

  // The actual Set of Joy, which changes during a resize event.  The
  // Only Field for this class, so I can atomically change the entire
  // set implementation with a single CAS.
  private transient NBSI _nbsi;

  public NonBlockingSetInt( ) { 
    _nbsi = new NBSI(63);       // The initial 1-word set
  }
    
  // Lower-case 'int' versions - no autoboxing, very fast.
  // Negative values are not allowed.
  public boolean add( final int i ) {
    if( i < 0 ) throw new IllegalArgumentException(""+i);
    return _nbsi.add(i);
  }
  public boolean remove  ( final int i ) { return _nbsi.remove  (i); }
  public boolean contains( final int i ) { return _nbsi.contains(i); }
  public int     size    (             ) { return _nbsi.size    ( ); }
  public void    clear   (             ) { 
    NBSI cleared = new NBSI(63);         // An empty initial NBSI
    while( !CAS_nbsi( _nbsi, cleared ) ) // Spin until clear works
      ;
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

  // Standard Java iterator.  Not terribly efficient.
  public Iterator<Integer> iterator( ) { return new iter(); }
  
  private class iter implements Iterator<Integer> {
    final NBSI _nbsi2;
    int _idx  = -1;
    int _prev = -1;
    iter() { _nbsi2 = _nbsi; advance(); }
    public boolean hasNext() { return _idx != -2; }
    private void advance() {
      while( true ) {
        _idx++;
        if( !_nbsi2.in_range(_idx) ) { _idx = -2; return; }
        if( _nbsi2.contains(_idx) ) return;
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
      _nbsi2.remove(_prev);
      _prev = -1;
    }
  }

  // --- writeObject -------------------------------------------------------
  // Write a NBSI to a stream
  private void writeObject(java.io.ObjectOutputStream s) throws IOException  {
    s.defaultWriteObject();     // Nothing to write
    final NBSI nbsi = _nbsi;    // The One Field is transient
    final int len = _nbsi._bits.length<<6;
    s.writeInt(len);            // Write max element
    for( int i=0; i<len; i++ ) 
      s.writeBoolean( _nbsi.contains(i) );
  }
  
  // --- readObject --------------------------------------------------------
  // Read a CHM from a stream
  private void readObject(java.io.ObjectInputStream s) throws IOException, ClassNotFoundException  {
    s.defaultReadObject();      // Read nothing
    final int len = s.readInt(); // Read max element
    _nbsi = new NBSI(len);
    for( int i=0; i<len; i++ )  // Read all bits
      if( s.readBoolean() )
        _nbsi.add(i);
  }

  // --- NBSI ----------------------------------------------------------------
  private static class NBSI {
    // Used to count elements: a high-performance counter.
    private transient final ConcurrentAutoTable _size;

    // The Bits
    private final long _bits[];
    // --- Bits to allow Unsafe access to arrays
    private static final int _Lbase  = _unsafe.arrayBaseOffset(long[].class);
    private static final int _Lscale = _unsafe.arrayIndexScale(long[].class);
    private static long rawIndex(final long[] ary, final int idx) {
      assert idx >= 0 && idx < ary.length;
      return _Lbase + idx * _Lscale;
    }
    private final boolean CAS( int idx, long old, long nnn ) {
      return _unsafe.compareAndSwapLong( _bits, rawIndex(_bits, idx), old, nnn );
    }

    private final boolean in_range( int idx ) {
      return (idx>>>6) < _bits.length;
    }
    private static final long mask( int i ) { return 1L<<(i&63); }

    // I need 1 free bit out of 64 to allow for resize.  I do this by stealing
    // the high order bit - but then I need to do something with adding element
    // number 63 (and friends).  I could use a mod63 function but it's more
    // efficient to handle the mod-64 case as an exception.
    //
    // Every 64th bit is put in it's own recursive bitvector.  If the low 6 bits
    // are all set, we shift them off and recursively operate on the _nbsi64 set.
    private final NBSI _nbsi64;
    
    private NBSI( int max_elem ) { 
      super(); 
      _bits = new long[(int)(((long)max_elem+63)>>>6)];
      _size = new ConcurrentAutoTable();
      _nbsi64 = ((max_elem+1)>>>6) == 0 ? null : new NBSI((max_elem+1)>>>6);
    }
    
    // Lower-case 'int' versions - no autoboxing, very fast.
    // 'i' is known positive.
    public boolean add( final int i ) {
      if( (i>>>6) >= _bits.length ) return resize_add(i);
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
    public boolean remove( final int i ) {
      if( i < 0 || (i>>>6) >= _bits.length ) return false;
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
      if( i < 0 || (i>>>6) >= _bits.length ) return false;
      if( (i&63) == 63 ) return _nbsi64.contains(i>>>6);
      return (_bits[i>>>6] & mask(i)) != 0;
    }
    
    public int size() { 
      return (int)_size.sum() + (_nbsi64==null ? 0 : _nbsi64.size()); 
    }

    public boolean resize_add( final int i ) {
      throw new Error("Unimplemented");
    }
  }    
}
