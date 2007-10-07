/*
 * Written by Cliff Click and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
import org.cliffc.high_scale_lib.*;
import java.util.*;

public class nbhs_tester extends Thread {
  static final String ss[] = new String[100];
  static { 
    for( int i=0; i<ss.length; i++ )
      ss[i] = "a"+i;
  }

  public static void main( String args[] ) throws Exception {
    NonBlockingHashSet<String> nbhs = new NonBlockingHashSet<String>();
    print(nbhs);

    if( nbhs.add(ss[3]) != true )
      throw new Exception("adding 3 but 3 should not exist");
    if( nbhs.add(ss[4]) != true )
      throw new Exception("adding 4 but 4 should not exist");
    print(nbhs);

    for( int i=0; i<ss.length; i++ )
      if( nbhs.add(ss[i]) != (i != 3 && i != 4) )
        throw new Exception("adding "+ss[i]+" but it should not exist");
    print(nbhs);

    nbhs.remove(ss[0]);
    if( nbhs.size() != ss.length-1 )
      throw new Exception("size off after remove");

    // simple iterator test
    for( String s : nbhs ) {
      System.out.print(s);
    }
    System.out.println();

    for (Iterator i = nbhs.iterator(); i.hasNext(); ) {
      String s = (String)i.next();
      i.remove();
      System.out.print(s);
    }
    System.out.println();
    print(nbhs);

    nbhs.clear();
    print(nbhs);
  }

  static void print(NonBlockingHashSet<String> nbhs) throws Exception {
    int size = nbhs.size();
    System.out.print("set["+size+"]=");
    if( (size==0) != nbhs.isEmpty() )
      throw new Exception("size="+size+", isEmpty="+nbhs.isEmpty());
    System.out.println(nbhs);
    //for( int i=0; i<ss.length; i++ )
    //  if( nbhs.contains(ss[i]) )
    //    System.out.print(""+ss[i]+",");
    //System.out.println("}");
  }
}
