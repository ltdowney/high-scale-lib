import junit.framework.Test;
import junit.framework.TestSuite;

public class JUnitSuite {
  public static Test suite() {
    TestSuite suite = new TestSuite();
    suite.addTestSuite(NBHM_Tester2.class);
    suite.addTestSuite(NBHML_Tester2.class);
    suite.addTestSuite(nbhs_Tester.class);
    suite.addTestSuite(nbsi_Tester.class);
    return suite;
  }
  public static void main(String[] args) { junit.textui.TestRunner.run(suite()); }
}
