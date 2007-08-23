rem javac -classpath "c:\Documents and Settings\Cliff\Desktop\Highly Scalable Java\high-scale-lib" perf_hash_test.java
javac -classpath "c:\Documents and Settings\Cliff\Desktop\Highly Scalable Java\high-scale-lib" perf_hashlong_test.java
java -classpath "c:\Documents and Settings\Cliff\Desktop\Highly Scalable Java\high-scale-lib;." -Xmx512m perf_hashlong_test 0 1 4 1 1000
