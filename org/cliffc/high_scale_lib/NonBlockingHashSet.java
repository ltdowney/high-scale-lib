/*
 * Written by Cliff Click and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

package org.cliffc.high_scale_lib;
import java.util.*;
import java.io.Serializable;

// A simple wrapper around NonBlockingHashMap making it a Set.  After calling
// the 'atomic_immutable' call the existing Set is made immutable (and it's
// internal format can change to better compress the data).

public class NonBlockingHashSet<E> extends AbstractSet<E> implements Serializable {
  static final Object V = "";

  private final NonBlockingHashMap<E,Object> _map;

  public NonBlockingHashSet() { super(); _map = new NonBlockingHashMap<E,Object>(); }

  public boolean add        ( final E          o ) { return _map.putIfAbsent(o,V) != V; }
  public boolean contains   ( final Object     o ) { return _map.containsKey(o); }
  public boolean remove     ( final Object     o ) { return _map.remove(o) == V; }
  public int     size       (                    ) { return _map.size(); }
  public void    clear      (                    ) { _map.clear(); }

  public Iterator<E>iterator(                    ) { return _map.keySet().iterator(); }

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
  public void atomicImmutable() {
    throw new RuntimeException("Unimplemented");
  }
}
