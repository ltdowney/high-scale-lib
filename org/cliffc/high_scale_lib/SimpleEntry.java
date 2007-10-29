/*
 * Written by Cliff Click and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

package org.cliffc.high_scale_lib;
import java.util.*;

// Inspired by JDK1.5, but different impl of 'setValue'
abstract class AbstractEntry<TypeK,TypeV> implements Map.Entry<TypeK,TypeV> {
  protected final TypeK _key;
  protected       TypeV _val;
  
  public AbstractEntry(final TypeK key, final TypeV val) { _key = key;        _val = val; }
  public AbstractEntry(final Map.Entry<TypeK,TypeV> e  ) { _key = e.getKey(); _val = e.getValue(); }
  public String toString() { return _key + "=" + _val; }
  public TypeK getKey  () { return _key;  }
  public TypeV getValue() { return _val;  }
  
  public boolean equals(final Object o) {
    if (!(o instanceof Map.Entry)) return false;
    final Map.Entry e = (Map.Entry)o;
    return eq(_key, e.getKey()) && eq(_val, e.getValue());
  }
  
  public int hashCode() {
    return 
      ((_key == null) ? 0 : _key.hashCode()) ^
      ((_val == null) ? 0 : _val.hashCode());
  }
  
  private static boolean eq(final Object o1, final Object o2) {
    return (o1 == null ? o2 == null : o1.equals(o2));
  }
}

