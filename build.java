
/*
 * Written by Cliff Click and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * A simple project build routine.

 * I hate makefiles, and ANT uses the horrid XML so I'm doing this hack
 * instead.  Actually, I've used this technique before to great success.
 * I ought to make a Real Project out of it someday.
 *
 * What you see is the 'guts' of make/ANT PLUS the project file dependencies
 * at the end - all in pure Java.  Thus I can add new features to the make
 * (parallel distributed make, shared caching, etc) or add new dependencies.
 * Especially for the dependencies, there is no obtuse format to describe
 * things - it's all pure Java.
 *
 *
 * @since 1.5
 * @author Cliff Click
 */
class build { 
  static private class BuildError extends Error { BuildError( String s ) { super(s); } }

  static boolean _verbose;
  static boolean _justprint;
  static boolean _keepontrucking;
  // Top-level project directory
  static File TOP;
  static String TOP_PATH;
  static String TOP_PATH_SLASH;

  // --- main ----------------------------------------------------------------
  static public void main( String args[] ) throws IOException {
    // --- First up: find build.java!
    // Where we exec'd java.exe from
    //String cwd = new File( '.' ).getCanonicalPath();
    TOP = new File(".");
    TOP_PATH = TOP.getCanonicalPath();
    File f = new File(TOP,"build.java");
    while( !f.exists() ) {
      File p2 = new File(TOP,"..");
      if( p2.getCanonicalPath().equals(TOP_PATH) )
        throw new BuildError("build.java not found; build.java marks top of project heirarchy");
      TOP = p2;
      TOP_PATH = TOP.getCanonicalPath();
      f = new File(TOP,"build.java");
    }

    TOP_PATH_SLASH = TOP_PATH.replaceAll("\\\\","\\\\\\\\");

    // --- Next up: always re-make self as needed
    if( _build_c.make() ) {
      // Since we remade ourself, launch & run self in a nested process to do
      // the actual 'build' using the new version of self.
      String a = "java build ";
      for( int i=0; i<args.length; i++ )
        a += args[i]+" ";
      ByteArrayOutputStream buf = sys_exec(a);
      buf.writeTo(System.out);
      System.exit(0);
    }

    // --- Strip out any flags; sanity check all targets before doing any of them
    int j = 0;
    boolean error = false;
    for( int i=0; i<args.length; i++ ) {
      if( args[i].charAt(0) == '-' ) {
        if( false ) ;
        else if( args[i].equals("-v") ) _verbose = true;
        else if( args[i].equals("-n") ) _justprint = true;
        else if( args[i].equals("-k") ) _keepontrucking = true;
        else {
          error = true;
          System.out.println("Unknown flag "+args[i]);
        }
      } else {
        if( Q.FILES.get(args[i]) == null ) {
          error = true;
          System.err.println("Unknown target "+args[i]);
        }
        args[j++] = args[i];
      }
    }
    if( error ) throw new Error("Command line errors");
    if( _verbose ) 
      System.out.println("Building in "+TOP.getCanonicalPath());

    // --- Build all named targets
    if( !_keepontrucking ) {
      // Build all till first error
      for( int i=0; i<j; i++ ) 
        Q.FILES.get(args[i]).make();
    } else {
      // Build all, keep going after failures
      try {
        for( int i=0; i<j; i++ ) 
          Q.FILES.get(args[i]).make();
      } catch( BuildError e ) {
        error = true;
        System.err.println(e);
      }
      if( error ) 
        throw new BuildError("Some build errors");
    }

    // --- All Done!
  }

  // --- StreamEater ---------------------------------------------------------
  static private class StreamEater extends Thread {
    final InputStream _is;
    final ByteArrayOutputStream _buf = new ByteArrayOutputStream();
    IOException _e;
    StreamEater( InputStream is ) { _is = is; start(); }
    public void run() {
      int len;
      byte[] buf = new byte[1024];
      try {
        while( (len=_is.read(buf)) != -1 ) {
          _buf.write(buf,0,len);
        }
      } catch( IOException e ) {
        _e = e;                 // Catch it for later, we're in the wrong thread
      }
    }
    public void close() throws IOException, InterruptedException {
      // called from the main thread on the StreamEater object, but not in the
      // StreamEater thread.
      join();
      if( _e != null ) throw _e; // Rethrow any exception in the main thread
    }
  }

  // --- sys_exec ------------------------------------------------------------
  // Run the command string as a new system process.  Throw an error if the
  // return value is not zero, or any number of other errors happen.
  static ByteArrayOutputStream sys_exec( String exec ) {
    if( exec.length() == 0 ) return null; // Vaciously works for empty commands
    Process p = null;
    StreamEater err = null, out = null;
    // This try/catch block will dump any output from the process before make dies
    try {
      // This try/catch block will catch any I/O errors and turn them into BuildErrors
      try {
        p = Runtime.getRuntime().exec(exec);
        err = new StreamEater(p.getErrorStream());
        out = new StreamEater(p.getInputStream());
        int status = p.waitFor();
        if( status != 0 ) 
          throw new BuildError("Status "+status+" from "+exec);
        out.close();            // catch runaway thread
        err.close();            // catch runaway thread
      } catch( IOException e ) {
        throw new BuildError("IOException from "+exec);
      } catch( InterruptedException e ) {
        throw new BuildError("Interrupted while waiting on "+exec);
      } finally {
        if( p != null ) p.destroy(); // catch runaway process
      }
    } catch( BuildError be ) {
      // Build-step choked.  Dump any output
      if( out != null ) try { out._buf.writeTo(System.out); } catch( IOException e ) { throw new BuildError(e.toString()); }
      if( err != null ) try { err._buf.writeTo(System.out); } catch( IOException e ) { throw new BuildError(e.toString()); }
      throw be;
    }
    return out._buf;           // No errors?  Then here is the buffered output
  }

  // --- A dependency --------------------------------------------------------
  static private class Q {

    // A table of all dependencies & target files in the world
    static ConcurrentHashMap<String,Q> FILES = new ConcurrentHashMap<String,Q>();

    // Basic definition of a dependency
    final String _target;
    final Q[] _srcs;            // Array of dependent files
    final String _exec;
    final char _src_sep;
    // These next fields are filled in lazily, as needed.
    String _parsed_exec;
    String _flat_src;
    File _dst;                  // Actual OS files
    long _modtime;           // Actual (or effective mod time for '-n' builds)

    // --- Constructor for a root file; no dependencies
    static final private Q[] NONE = new Q[0];
    Q( String target ) {
      _target = target;
      _exec = "";               // no exec string
      _parsed_exec = "";
      _src_sep = ' ';
      _srcs = NONE;
      init();
    }

    // --- Constructor for a single dependency
    Q( String target, Q src, String exec ) {
      _target = target;
      _exec = exec;
      _src_sep = ' ';
      _srcs = new Q[1];
      _srcs[0] = src;
      init();
    }

    // --- Constructor for a list of source dependencies
    Q( String target, Q[] srcs, char src_sep, String exec ) {
      // Fill in the fields
      _target = target;
      _exec = exec;
      _src_sep = src_sep;
      _srcs = srcs;
      init();
    }

    private void init() {
      // Basic sanity checking
      if( _target.indexOf('%') != -1 ) 
        throw new IllegalArgumentException("dependency target has a '%': "+_target);
      for( int i=_exec.indexOf('%'); i!= -1; i = _exec.indexOf('%',i+1) ) {
        if( false ) {
        } else if( _exec.startsWith("src",i+1) ) { 
        } else if( _exec.startsWith("dst",i+1) ) { 
        } else if( _exec.startsWith("top",i+1) ) { 
        } else
          throw new IllegalArgumentException("dependency exec has unknown pattern: "+_exec.substring(i));
      }
      _flat_src = flat_src(',');

      // Install the target/dependency mapping in a flat table
      if( FILES.put(_target,this) != null ) 
        throw new IllegalArgumentException("More than one dependency for target "+_target);
    }

    String flat_src( char sep ) {
      String s = "";
      if( _srcs.length==0 ) return s;
      for( int i=0; i<_srcs.length-1; i++ )
        s += _srcs[i]._target+sep;
      s += _srcs[_srcs.length-1]._target;
      return s;
    }

    // --- parse_exec
    // The _exec String contains normal text, plus '%src' and '%dst' strings.
    String parse_exec() {
      if( _parsed_exec == null )
        _parsed_exec = _exec
          .replaceAll("%src",TOP_PATH_SLASH+"/"+flat_src(_src_sep))
          .replaceAll("%dst",TOP_PATH_SLASH+"/"+_target)
          .replaceAll("%top",TOP_PATH_SLASH);
      return _parsed_exec;
    }

    // True if our file modtime is after all source file mod times.
    // Correctly reports "not up to date" if modtime is not yet init'd.
    final long latest() {
      // See if we are already up-to-date.  Ugh, time only accurate to milli-
      // seconds.  Build if times are equal, because I can't tell who's on first.
      long l = 0;
      for( int i=0; i<_srcs.length; i++ )
        if( l < _srcs[i]._modtime )
          l = _srcs[i]._modtime;
      return l;
    }

    protected ByteArrayOutputStream do_it( String exec ) {
      System.out.print(exec);
      return sys_exec(exec);
    }

    // --- make
    // Run the 'exec' string if the source file is more recent than the target file.
    // Return true if the target file (apparently) got updated.
    final boolean make() {
      // See if we are already up-to-date.
      // Check for before checking for init'ing modtime as a speed hack.
      if( _modtime > latest() ) {
        if( _verbose ) System.out.println(_target+ " > {" +_flat_src + "} : already up to date");
        return false;           // Update-to-date, nothing changed
      }
      
      // Recursively make required files
      boolean anychanges = false;
      for( int i=0; i<_srcs.length; i++ )
        if( _srcs[i].make() )
          anychanges = true;

      // Back target string with an OS file
      if( _dst == null ) _dst = new File(TOP,_target);
      // Re-read, in case changed.  Last ditch effort to start an expensive process.
      // But do not let _modtime roll backwards in any case.
      long t = _dst.lastModified(); 
      if( t > _modtime ) _modtime = t;

      long last_src = latest(); // Lastest source-file time
      if( !anychanges && _modtime > last_src ) { // Ahhh, all is well
        if( _verbose ) System.out.println(_target+ " > {" +_flat_src + "} : already up to date");
        return false;
      }

      // Files out of date; must do this build step
      String exec = parse_exec();
      if( _verbose ) System.out.println(_target+ " <= {"+_flat_src+"}");
      if( _justprint ) {
        System.out.println(exec);
        _modtime = last_src+1;  // Force modtime update
        return true;
      }
      
      // Actually do the step (make the sys-call)
      try {
        do_it(exec);
      } catch( BuildError be ) {
        if( this != _build_c )  // Highly annoying to delete own class file
          _dst.delete();        // Failed; make sure target is deleted (except for build.class)
        throw be;
      } finally {
        System.out.println();
      }

      // Double-check that the source files were not modified by the
      // build-step.  It's a fairly common error if file-names get swapped,
      // etc.  This exception is uncaught, so it aborts everything.  It
      // basically indicate a broken build file - the build-step is changing
      // the wrong file.
      for( int i=0; i<_srcs.length; i++ )
        if( _srcs[i]._modtime < _srcs[i]._dst.lastModified() )
          throw new IllegalArgumentException("Timestamp for source file "+_srcs[i]._target+" apparently advanced by exec'ing "+_exec);

      // Double-check that this step made progress.  Again, failure here is
      // likely a build-step failure to modify the correct file.
      long x = _dst.lastModified();
      if( _modtime == x )
        throw new IllegalArgumentException("Timestamp for "+_target+" not changed by exec'ing "+_exec);
      
      // For very fast build-steps, the target may be updated to a time equal
      // to the input file times after rounding to the file-system's time
      // precision - which might be as bad as 1 whole second.  Assume the
      // build step worked, but give the result an apparent time just past the
      // src file times to make later steps more obvious.
      if( x < last_src )
        throw new IllegalArgumentException("Timestamp of "+_target+" not moved past {"+_flat_src+"} timestamps by exec'ing "+_exec);
      if( x == last_src ) {     // Aaahh, we have 'tied' in the timestamps
        long now = System.currentTimeMillis();
        while( now <= x ) {     // Stall till real-time is in a new milli-second
          try { Thread.sleep(1); } catch( InterruptedException e ) { };
          now = System.currentTimeMillis();
        }
        x = now;                // Pretend file was made 'right NOW!'
      }
      _modtime = x;             // Record apparent mod-time
      
      return true;
    }

  };

  // --- A dependency for a JUnit test ---------------------------------------
  // Mostly just a normal dependency, except it writes the testing output
  // to a log file.
  static private class Q_JUnit extends Q {
    final String _base;
    Q_JUnit( String base, Q clazzfile, String exec ) {
      super(base+".log",clazzfile,exec);
      _base = base;
    }

    protected ByteArrayOutputStream do_it( String exec ) {
      ByteArrayOutputStream out = super.do_it(exec);
      // If we can 'do_it' with no errors, then dump the output to a base.log file
      try { 
        File f = new File(TOP_PATH_SLASH+"/"+_base+".log");
        FileOutputStream log = new FileOutputStream(f);
        out.writeTo(log);
        log.close();
        System.out.print(" > "+f.getCanonicalPath());
      } catch( FileNotFoundException e ) {
        throw new BuildError("Unable to create file "+_base+".log, "+e.toString());
      } catch( IOException e ) {
        throw new BuildError("Error while file "+_base+".log, "+e.toString());
      }
      return out;
    }
  }
  


  // =========================================================================
  // --- The Dependencies ----------------------------------------------------
  // =========================================================================

  // Some common strings
  static final Q[] NONE = new Q[0];
  static final String javac = "javac -cp %top %src";
  static final String javadoc = "javadoc -quiet -classpath %top -d %top/doc -package -link http://java.sun.com/j2se/1.5.0/docs/api %src";

  // The build-self dependency every project needs
  static final Q _build_j = new Q("build.java");
  static final Q _build_c = new Q("build.class",_build_j,"javac %src");

  // The High Scale Lib java files
  static final String HSL = "org/cliffc/high_scale_lib";
  static final Q _absen_j = new Q(HSL+"/AbstractEntry.java");
  static final Q _cat_j   = new Q(HSL+"/ConcurrentAutoTable.java");
  static final Q _cntr_j  = new Q(HSL+"/Counter.java");
  static final Q _nbhm_j  = new Q(HSL+"/NonBlockingHashMap.java");
  static final Q _nbhml_j = new Q(HSL+"/NonBlockingHashMapLong.java");
  static final Q _nbhs_j  = new Q(HSL+"/NonBlockingHashSet.java");
  static final Q _nbsi_j  = new Q(HSL+"/NonBlockingSetInt.java");
  static final Q _unsaf_j = new Q(HSL+"/UtilUnsafe.java");

  // The High Scale Lib class files
  static final Q _absen_cls = new Q(HSL+"/AbstractEntry.class"         , _absen_j, javac);
  static final Q _cat_cls   = new Q(HSL+"/ConcurrentAutoTable.class"   , _cat_j  , javac); 
  static final Q _cntr_cls  = new Q(HSL+"/Counter.class"               , _cntr_j , javac);              
  static final Q _nbhm_cls  = new Q(HSL+"/NonBlockingHashMap.class"    , _nbhm_j , javac);
  static final Q _nbhml_cls = new Q(HSL+"/NonBlockingHashMapLong.class", _nbhml_j, javac);
  static final Q _nbhs_cls  = new Q(HSL+"/NonBlockingHashSet.class"    , _nbhs_j , javac);
  static final Q _nbsi_cls  = new Q(HSL+"/NonBlockingSetInt.class"     , _nbsi_j , javac);
  static final Q _unsaf_cls = new Q(HSL+"/UtilUnsafe.class"            , _unsaf_j, javac);

  // The testing files.  JUnit output is in a corresponding .log file.
  static final String TNBHM = "Testing/NBHM_Tester";
  static final Q _tnbhm_j   = new Q(TNBHM+"/NBHM_Tester2.java");
  static final Q _tnbhm_cls = new Q(TNBHM+"/NBHM_Tester2.class", _tnbhm_j  , "javac -cp %top:%top/junit-4.4.jar %src");
  static final Q _tnbhm_tst = new Q_JUnit(TNBHM+"/NBHM_Tester2", _tnbhm_cls, "java  -cp %top:%top/junit-4.4.jar:%top/"+TNBHM+" NBHM_Tester2");
  static final Q _tnbhml_j  = new Q(TNBHM+"/NBHML_Tester2.java");
  static final Q _tnbhml_cls= new Q(TNBHM+"/NBHML_Tester2.class",_tnbhml_j  ,"javac -cp %top:%top/junit-4.4.jar %src");
  static final Q _tnbhml_tst= new Q_JUnit(TNBHM+"/NBHML_Tester2",_tnbhml_cls,"java  -cp %top:%top/junit-4.4.jar:%top/"+TNBHM+" NBHML_Tester2");

  static final String TNBHS = "Testing/NBHS_Tester";
  static final Q _tnbhs_j   = new Q(TNBHS+"/nbhs_tester.java");
  static final Q _tnbhs_cls = new Q(TNBHS+"/nbhs_tester.class", _tnbhs_j  , "javac -cp %top:%top/junit-4.4.jar %src");
  static final Q _tnbhs_tst = new Q_JUnit(TNBHS+"/nbhs_tester", _tnbhs_cls, "java  -cp %top:%top/junit-4.4.jar:%top/"+TNBHS+" nbhs_tester");
  static final Q _tnbsi_j   = new Q(TNBHS+"/nbsi_tester.java");
  static final Q _tnbsi_cls = new Q(TNBHS+"/nbsi_tester.class", _tnbsi_j  , "javac -cp %top:%top/junit-4.4.jar %src");
  static final Q _tnbsi_tst = new Q_JUnit(TNBHS+"/nbsi_tester", _tnbsi_cls, "java  -cp %top:%top/junit-4.4.jar:%top/"+TNBHS+" nbsi_tester");

  // The high-scale-lib.jar file.  Demand JUnit testing in addition to class files.
  static final Q[] _clss = { _absen_cls, _cat_cls, _cntr_cls, _tnbhm_tst, _tnbhml_tst, _tnbhs_tst, _tnbsi_tst, _unsaf_cls };
  static final Q _hsl_jar = new Q("high-scale-lib.jar",_clss,' ',"jar -cf %dst %top/"+HSL);

  // The High Scale Lib javadoc files
  static final String HSLDOC = "doc/"+HSL;
  static final Q _absen_doc= new Q(HSLDOC+"/AbstractEntry.html"         , _absen_j, javadoc);
  static final Q _cat_doc  = new Q(HSLDOC+"/ConcurrentAutoTable.html"   , _cat_j  , javadoc); 
  static final Q _cntr_doc = new Q(HSLDOC+"/Counter.html"               , _cntr_j , javadoc);              
  static final Q _nbhm_doc = new Q(HSLDOC+"/NonBlockingHashMap.html"    , _nbhm_j , javadoc);
  static final Q _nbhml_doc= new Q(HSLDOC+"/NonBlockingHashMapLong.html", _nbhml_j, javadoc);
  static final Q _nbhs_doc = new Q(HSLDOC+"/NonBlockingHashSet.html"    , _nbhs_j , javadoc);
  static final Q _nbsi_doc = new Q(HSLDOC+"/NonBlockingSetInt.html"     , _nbsi_j , javadoc);
  static final Q _unsaf_doc= new Q(HSLDOC+"/UtilUnsafe.html"            , _unsaf_j, javadoc);

  static final Q[] _docs = { _absen_doc, _cat_doc, _cntr_doc, _nbhm_doc, _nbhml_doc, _nbhs_doc, _nbsi_doc, _unsaf_doc };
  static final Q _dummy_doc= new Q("doc/dummy",_docs, ' ', "touch %dst");
}
