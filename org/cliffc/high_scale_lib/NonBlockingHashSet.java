/*
 * Written by Cliff Click and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

package org.cliffc.high_scale_lib;
import java.util.*;

// A simple wrapper around NonBlockingHashMap making it a Set, with
// key==value.  After calling the 'immutable' call the existing Set is
// made immutable (and it's internal format can change to better
// compress the data).

public class NonBlockingHashSet<E> implements Set<E> {

  private NonBlockingHashMap<E,E> _map;

  public NonBlockingHashSet() { _map = new NonBlockingHashMap<E,E>(); }

  public boolean add        ( final E          o ) { return _map.putIfAbsent(o,o) != o; }
  public boolean contains   ( final Object     o ) { return _map.containsKey(o); }
  public boolean isEmpty    (                    ) { return _map.size() == 0; }
  public boolean remove     ( final Object     o ) { return _map.remove(o,o) == o; }
  public int     size       (                    ) { return _map.size(); }
  public void    clear      (                    ) { _map.clear(); }

  public Iterator<E>iterator(                    ) { return new SetIterator<E>(); }

  public <T> T[] toArray    ( final T[]        a ) { throw new RuntimeException("not implemented"); }
  public Object[]toArray    (                    ) { throw new RuntimeException("not implemented"); }
  public boolean addAll     ( final Collection<? extends E> c ) { throw new RuntimeException("not implemented"); }
  public boolean containsAll( final Collection c ) { throw new RuntimeException("not implemented"); }
  public boolean removeAll  ( final Collection c ) { throw new RuntimeException("not implemented"); }
  public boolean retainAll  ( final Collection c ) { throw new RuntimeException("not implemented"); }

  // ---

  // Make the Set immutable.  Future calls to mutate will throw an
  // IllegalStateException.  Existing mutator calls will race with this thread
  // and will either throw IllegalStateException or have their update made
  // visible.  This means that a simple flag-set won't make the Set immutable,
  // because a late-arriving update might pass the immutable flag-set, then
  // mutate the Set after the 'immutable()' call returns.  This call can be
  // called concurrently (and indeed until the operation completes, all calls
  // on the set from any thread either complete normally or end up calling
  // 'immutable' internally).

  // (1) call _map's immutable() call
  // (2) get snapshot
  // (3) CAS down a local map, power-of-2 larger than _map.size()+1/8th
  // (4) start @ random, visit all snapshot, insert live keys
  // (5) CAS _map to null, needs happens-after (4)
  // (6) if Set call sees _map is null, needs happens-after (4) for readers
  public void immutable() {

  }

  // ---
  // This iterator is NOT multi-threaded safe per-se.  Multiple callers of the
  // same iterator will confuse the underlying variables and can crash the
  // Iterator.  

  // The Iterator only guarantees to visit those elements that exist over the
  // entire lifetime of iteration.  Even if this iterator is always called
  // single-threaded, the underlying Set can be concurrently modified.
  // Elements inserted or removed during iteration (by this or another thread)
  // may or may not be visited.  However, if no other thread is modifying the
  // set and this thread is not adding members and only deleting members found
  // with next() (or by calling remove()), then the iterator is guaranteed to
  // visit all members.  i.e., the Set can be used as a worklist.
  final private class SetIterator<E> implements Iterator<E> {
    private final NonBlockingHashMap.Snapshot<E,E> _ss;
    private int _idx;                   // Index
    private Object _next;               // Next found element
    private Object _toberemoved;
    SetIterator() {
      _ss = _map.snapshot();    // Get a snapshot
      next();                   // Setup for 'next' call
    }
    public boolean hasNext() { return _next != null; }
    public void remove() { 
      if( _toberemoved == null ) throw new IllegalStateException(); 
      _map.remove(_toberemoved);  
      _toberemoved = null;
    }
    public E next() { 
      final Object nn = _next;  // The definite 'next' value to be returned
      if( nn == null ) throw NoSuchElementException();
      _next = null;             // But now find the next 'next'
      while( _idx<_ss.length() ) { // Scan array
        _next = _ss.key(_idx++);  // Get a key that definitely is in the set (for the moment!)
        if( _next != null  )    // Found something?
          break;
      }                         // Else keep scanning
      _toberemoved = nn;
      return (E)nn;             // Return 'next' value.  Note annoying runtime cast.
    }
  }
}
