/*
 * Written by Cliff Click and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

package org.cliffc.high_scale_lib;
import java.util.*;

// A simple wrapper around NonBlockingHashMap to make it a Set, with key==value.

public class NonBlockingHashSet<E> implements Set<E> {

  private final NonBlockingHashMap<E,E> _map;

  public NonBlockingHashSet() { _map = new NonBlockingHashMap<E,E>(); }

  public boolean add        ( E          o ) { return _map.putIfAbsent(o,o) != o; }
  public boolean contains   ( Object     o ) { return _map.containsKey(o); }
  public boolean isEmpty    (              ) { return _map.size() == 0; }
  public boolean remove     ( Object     o ) { return _map.remove(o) == o; }
  public int     size       (              ) { return _map.size(); }
  public void    clear      (              ) { _map.clear(); }
  
  public <T> T[] toArray    ( T[]        a ) { throw new RuntimeException("not implemented"); }
  public Iterator<E>iterator(              ) { throw new RuntimeException("not implemented"); }
  public Object[]toArray    (              ) { throw new RuntimeException("not implemented"); }
  public boolean addAll     ( Collection<? extends E> c ) { throw new RuntimeException("not implemented"); }
  public boolean containsAll( Collection c ) { throw new RuntimeException("not implemented"); }
  public boolean removeAll  ( Collection c ) { throw new RuntimeException("not implemented"); }
  public boolean retainAll  ( Collection c ) { throw new RuntimeException("not implemented"); }
}
