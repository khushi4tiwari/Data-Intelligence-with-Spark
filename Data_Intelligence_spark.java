package project_test_90;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Vector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import java.util.*;


//import Revisionutils.Type;

//import weka.core.*;

class Stopwords implements RevisionHandler {

	  /** The hash set containing the list of stopwords */
	  protected HashSet<String> m_Words = null;

	  /** The default stopwords object (stoplist based on Rainbow) */
	  protected static Stopwords m_Stopwords;

	  static {
	    if (m_Stopwords == null) {
	      m_Stopwords = new Stopwords();
	    }
	  }

	  /**
	   * initializes the stopwords (based on <a
	   * href="http://www.cs.cmu.edu/~mccallum/bow/rainbow/"
	   * target="_blank">Rainbow</a>).
	   */
	  public Stopwords() {
	    m_Words = new HashSet<String>();

	    // Stopwords list from Rainbow
	    add("a");
	    add("able");
	    add("about");
	    add("above");
	    add("according");
	    add("accordingly");
	    add("across");
	    add("actually");
	    add("after");
	    add("afterwards");
	    add("again");
	    add("against");
	    add("all");
	    add("allow");
	    add("allows");
	    add("almost");
	    add("alone");
	    add("along");
	    add("already");
	    add("also");
	    add("although");
	    add("always");
	    add("am");
	    add("among");
	    add("amongst");
	    add("an");
	    add("and");
	    add("another");
	    add("any");
	    add("anybody");
	    add("anyhow");
	    add("anyone");
	    add("anything");
	    add("anyway");
	    add("anyways");
	    add("anywhere");
	    add("apart");
	    add("appear");
	    add("appreciate");
	    add("appropriate");
	    add("are");
	    add("around");
	    add("as");
	    add("aside");
	    add("ask");
	    add("asking");
	    add("associated");
	    add("at");
	    add("available");
	    add("away");
	    add("awfully");
	    add("b");
	    add("be");
	    add("became");
	    add("because");
	    add("become");
	    add("becomes");
	    add("becoming");
	    add("been");
	    add("before");
	    add("beforehand");
	    add("behind");
	    add("being");
	    add("believe");
	    add("below");
	    add("beside");
	    add("besides");
	    add("best");
	    add("better");
	    add("between");
	    add("beyond");
	    add("both");
	    add("brief");
	    add("but");
	    add("by");
	    add("c");
	    add("came");
	    add("can");
	    add("cannot");
	    add("cant");
	    add("cause");
	    add("causes");
	    add("certain");
	    add("certainly");
	    add("changes");
	    add("clearly");
	    add("co");
	    add("com");
	    add("come");
	    add("comes");
	    add("concerning");
	    add("consequently");
	    add("consider");
	    add("considering");
	    add("contain");
	    add("containing");
	    add("contains");
	    add("corresponding");
	    add("could");
	    add("course");
	    add("currently");
	    add("d");
	    add("definitely");
	    add("described");
	    add("despite");
	    add("did");
	    add("different");
	    add("do");
	    add("does");
	    add("doing");
	    add("done");
	    add("down");
	    add("downwards");
	    add("during");
	    add("e");
	    add("each");
	    add("edu");
	    add("eg");
	    add("eight");
	    add("either");
	    add("else");
	    add("elsewhere");
	    add("enough");
	    add("entirely");
	    add("especially");
	    add("et");
	    add("etc");
	    add("even");
	    add("ever");
	    add("every");
	    add("everybody");
	    add("everyone");
	    add("everything");
	    add("everywhere");
	    add("ex");
	    add("exactly");
	    add("example");
	    add("except");
	    add("f");
	    add("far");
	    add("few");
	    add("fifth");
	    add("first");
	    add("five");
	    add("followed");
	    add("following");
	    add("follows");
	    add("for");
	    add("former");
	    add("formerly");
	    add("forth");
	    add("four");
	    add("from");
	    add("further");
	    add("furthermore");
	    add("g");
	    add("get");
	    add("gets");
	    add("getting");
	    add("given");
	    add("gives");
	    add("go");
	    add("goes");
	    add("going");
	    add("gone");
	    add("got");
	    add("gotten");
	    add("greetings");
	    add("h");
	    add("had");
	    add("happens");
	    add("hardly");
	    add("has");
	    add("have");
	    add("having");
	    add("he");
	    add("hello");
	    add("help");
	    add("hence");
	    add("her");
	    add("here");
	    add("hereafter");
	    add("hereby");
	    add("herein");
	    add("hereupon");
	    add("hers");
	    add("herself");
	    add("hi");
	    add("him");
	    add("himself");
	    add("his");
	    add("hither");
	    add("hopefully");
	    add("how");
	    add("howbeit");
	    add("however");
	    add("i");
	    add("ie");
	    add("if");
	    add("ignored");
	    add("immediate");
	    add("in");
	    add("inasmuch");
	    add("inc");
	    add("indeed");
	    add("indicate");
	    add("indicated");
	    add("indicates");
	    add("inner");
	    add("insofar");
	    add("instead");
	    add("into");
	    add("inward");
	    add("is");
	    add("it");
	    add("its");
	    add("itself");
	    add("j");
	    add("just");
	    add("k");
	    add("keep");
	    add("keeps");
	    add("kept");
	    add("know");
	    add("knows");
	    add("known");
	    add("l");
	    add("last");
	    add("lately");
	    add("later");
	    add("latter");
	    add("latterly");
	    add("least");
	    add("less");
	    add("lest");
	    add("let");
	    add("like");
	    add("liked");
	    add("likely");
	    add("little");
	    add("ll"); // added to avoid words like you'll,I'll etc.
	    add("look");
	    add("looking");
	    add("looks");
	    add("ltd");
	    add("m");
	    add("mainly");
	    add("many");
	    add("may");
	    add("maybe");
	    add("me");
	    add("mean");
	    add("meanwhile");
	    add("merely");
	    add("might");
	    add("more");
	    add("moreover");
	    add("most");
	    add("mostly");
	    add("much");
	    add("must");
	    add("my");
	    add("myself");
	    add("n");
	    add("name");
	    add("namely");
	    add("nd");
	    add("near");
	    add("nearly");
	    add("necessary");
	    add("need");
	    add("needs");
	    add("neither");
	    add("never");
	    add("nevertheless");
	    add("new");
	    add("next");
	    add("nine");
	    add("no");
	    add("nobody");
	    add("non");
	    add("none");
	    add("noone");
	    add("nor");
	    add("normally");
	    add("not");
	    add("nothing");
	    add("novel");
	    add("now");
	    add("nowhere");
	    add("o");
	    add("obviously");
	    add("of");
	    add("off");
	    add("often");
	    add("oh");
	    add("ok");
	    add("okay");
	    add("old");
	    add("on");
	    add("once");
	    add("one");
	    add("ones");
	    add("only");
	    add("onto");
	    add("or");
	    add("other");
	    add("others");
	    add("otherwise");
	    add("ought");
	    add("our");
	    add("ours");
	    add("ourselves");
	    add("out");
	    add("outside");
	    add("over");
	    add("overall");
	    add("own");
	    add("p");
	    add("particular");
	    add("particularly");
	    add("per");
	    add("perhaps");
	    add("placed");
	    add("please");
	    add("plus");
	    add("possible");
	    add("presumably");
	    add("probably");
	    add("provides");
	    add("q");
	    add("que");
	    add("quite");
	    add("qv");
	    add("r");
	    add("rather");
	    add("rd");
	    add("re");
	    add("really");
	    add("reasonably");
	    add("regarding");
	    add("regardless");
	    add("regards");
	    add("relatively");
	    add("respectively");
	    add("right");
	    add("s");
	    add("said");
	    add("same");
	    add("saw");
	    add("say");
	    add("saying");
	    add("says");
	    add("second");
	    add("secondly");
	    add("see");
	    add("seeing");
	    add("seem");
	    add("seemed");
	    add("seeming");
	    add("seems");
	    add("seen");
	    add("self");
	    add("selves");
	    add("sensible");
	    add("sent");
	    add("serious");
	    add("seriously");
	    add("seven");
	    add("several");
	    add("shall");
	    add("she");
	    add("should");
	    add("since");
	    add("six");
	    add("so");
	    add("some");
	    add("somebody");
	    add("somehow");
	    add("someone");
	    add("something");
	    add("sometime");
	    add("sometimes");
	    add("somewhat");
	    add("somewhere");
	    add("soon");
	    add("sorry");
	    add("specified");
	    add("specify");
	    add("specifying");
	    add("still");
	    add("sub");
	    add("such");
	    add("sup");
	    add("sure");
	    add("t");
	    add("take");
	    add("taken");
	    add("tell");
	    add("tends");
	    add("th");
	    add("than");
	    add("thank");
	    add("thanks");
	    add("thanx");
	    add("that");
	    add("thats");
	    add("the");
	    add("their");
	    add("theirs");
	    add("them");
	    add("themselves");
	    add("then");
	    add("thence");
	    add("there");
	    add("thereafter");
	    add("thereby");
	    add("therefore");
	    add("therein");
	    add("theres");
	    add("thereupon");
	    add("these");
	    add("they");
	    add("think");
	    add("third");
	    add("this");
	    add("thorough");
	    add("thoroughly");
	    add("those");
	    add("though");
	    add("three");
	    add("through");
	    add("throughout");
	    add("thru");
	    add("thus");
	    add("to");
	    add("together");
	    add("too");
	    add("took");
	    add("toward");
	    add("towards");
	    add("tried");
	    add("tries");
	    add("truly");
	    add("try");
	    add("trying");
	    add("twice");
	    add("two");
	    add("u");
	    add("un");
	    add("under");
	    add("unfortunately");
	    add("unless");
	    add("unlikely");
	    add("until");
	    add("unto");
	    add("up");
	    add("upon");
	    add("us");
	    add("use");
	    add("used");
	    add("useful");
	    add("uses");
	    add("using");
	    add("usually");
	    add("uucp");
	    add("v");
	    add("value");
	    add("various");
	    add("ve"); // added to avoid words like I've,you've etc.
	    add("very");
	    add("via");
	    add("viz");
	    add("vs");
	    add("w");
	    add("want");
	    add("wants");
	    add("was");
	    add("way");
	    add("we");
	    add("welcome");
	    add("well");
	    add("went");
	    add("were");
	    add("what");
	    add("whatever");
	    add("when");
	    add("whence");
	    add("whenever");
	    add("where");
	    add("whereafter");
	    add("whereas");
	    add("whereby");
	    add("wherein");
	    add("whereupon");
	    add("wherever");
	    add("whether");
	    add("which");
	    add("while");
	    add("whither");
	    add("who");
	    add("whoever");
	    add("whole");
	    add("whom");
	    add("whose");
	    add("why");
	    add("will");
	    add("willing");
	    add("wish");
	    add("with");
	    add("within");
	    add("without");
	    add("wonder");
	    add("would");
	    add("would");
	    add("x");
	    add("y");
	    add("yes");
	    add("yet");
	    add("you");
	    add("your");
	    add("yours");
	    add("yourself");
	    add("yourselves");
	    add("z");
	    add("zero");
	  }

	  /**
	   * removes all stopwords
	   */
	  public void clear() {
	    m_Words.clear();
	  }

	  /**
	   * adds the given word to the stopword list (is automatically converted to
	   * lower case and trimmed)
	   * 
	   * @param word the word to add
	   */
	  public void add(String word) {
	    if (word.trim().length() > 0) {
	      m_Words.add(word.trim().toLowerCase());
	    }
	  }

	  /**
	   * removes the word from the stopword list
	   * 
	   * @param word the word to remove
	   * @return true if the word was found in the list and then removed
	   */
	  public boolean remove(String word) {
	    return m_Words.remove(word);
	  }

	  /**
	   * Returns true if the given string is a stop word.
	   * 
	   * @param word the word to test
	   * @return true if the word is a stopword
	   */
	  public boolean is(String word) {
	    return m_Words.contains(word.toLowerCase());
	  }

	  /**
	   * Returns a sorted enumeration over all stored stopwords
	   * 
	   * @return the enumeration over all stopwords
	   */
	  public Enumeration<String> elements() {

	    Vector<String> list = new Vector<String>();

	    list.addAll(m_Words);

	    // sort list
	    Collections.sort(list);

	    return list.elements();
	  }

	  /**
	   * Generates a new Stopwords object from the given file
	   * 
	   * @param filename the file to read the stopwords from
	   * @throws Exception if reading fails
	   */
	  public void read(String filename) throws Exception {
	    read(new File(filename));
	  }

	  /**
	   * Generates a new Stopwords object from the given file
	   * 
	   * @param file the file to read the stopwords from
	   * @throws Exception if reading fails
	   */
	  public void read(File file) throws Exception {
	    read(new BufferedReader(new FileReader(file)));
	  }

	  /**
	   * Generates a new Stopwords object from the reader. The reader is closed
	   * automatically.
	   * 
	   * @param reader the reader to get the stopwords from
	   * @throws Exception if reading fails
	   */
	  public void read(BufferedReader reader) throws Exception {
	    String line;

	    clear();

	    while ((line = reader.readLine()) != null) {
	      line = line.trim();
	      // comment?
	      if (line.startsWith("#")) {
	        continue;
	      }
	      add(line);
	    }

	    reader.close();
	  }

	  /**
	   * Writes the current stopwords to the given file
	   * 
	   * @param filename the file to write the stopwords to
	   * @throws Exception if writing fails
	   */
	  public void write(String filename) throws Exception {
	    write(new File(filename));
	  }

	  /**
	   * Writes the current stopwords to the given file
	   * 
	   * @param file the file to write the stopwords to
	   * @throws Exception if writing fails
	   */
	  public void write(File file) throws Exception {
	    write(new BufferedWriter(new FileWriter(file)));
	  }

	  /**
	   * Writes the current stopwords to the given writer. The writer is closed
	   * automatically.
	   * 
	   * @param writer the writer to get the stopwords from
	   * @throws Exception if writing fails
	   */
	  public void write(BufferedWriter writer) throws Exception {
	    Enumeration<String> enm;

	    // header
	    writer.write("# generated " + new Date());
	    writer.newLine();

	    enm = elements();

	    while (enm.hasMoreElements()) {
	      writer.write(enm.nextElement().toString());
	      writer.newLine();
	    }

	    writer.flush();
	    writer.close();
	  }

	  /**
	   * returns the current stopwords in a string
	   * 
	   * @return the current stopwords
	   */
	  @Override
	  public String toString() {
	    Enumeration<String> enm;
	    StringBuffer result;

	    result = new StringBuffer();
	    enm = elements();
	    while (enm.hasMoreElements()) {
	      result.append(enm.nextElement().toString());
	      if (enm.hasMoreElements()) {
	        result.append(",");
	      }
	    }

	    return result.toString();
	  }

	  /**
	   * Returns true if the given string is a stop word.
	   * 
	   * @param str the word to test
	   * @return true if the word is a stopword
	   */
	  public static boolean isStopword(String str) {
	    return m_Stopwords.is(str.toLowerCase());
	  }

	  /**
	   * Returns the revision string.
	   * 
	   * @return the revision
	   */
	  //@Override
	  Revisionutils obj=new Revisionutils();
	  public String getRevision() {
	    return Revisionutils.extract("$Revision: 10203 $");
	  }

	  /**
	   * Accepts the following parameter:
	   * <p/>
	   * 
	   * -i file <br/>
	   * loads the stopwords from the given file
	   * <p/>
	   * 
	   * -o file <br/>
	   * saves the stopwords to the given file
	   * <p/>
	   * 
	   * -p <br/>
	   * outputs the current stopwords on stdout
	   * <p/>
	   * 
	   * Any additional parameters are interpreted as words to test as stopwords.
	   * 
	   * @param args commandline parameters
	   * @throws Exception if something goes wrong
	   *//*
	  public static void main(String[] args) throws Exception {
	    String input = Utils.getOption('i', args);
	    String output = Utils.getOption('o', args);
	    boolean print = Utils.getFlag('p', args);

	    // words to process?
	    Vector<String> words = new Vector<String>();
	    for (String arg : args) {
	      if (arg.trim().length() > 0) {
	        words.add(arg.trim());
	      }
	    }

	    Stopwords stopwords = new Stopwords();

	    // load from file?
	    if (input.length() != 0) {
	      stopwords.read(input);
	    }

	    // write to file?
	    if (output.length() != 0) {
	      stopwords.write(output);
	    }

	    // output to stdout?
	    if (print) {
	      System.out.println("\nStopwords:");
	      Enumeration<String> enm = stopwords.elements();
	      int i = 0;
	      while (enm.hasMoreElements()) {
	        System.out.println((i + 1) + ". " + enm.nextElement());
	        i++;
	      }
	    }

	    // check words for being a stopword
	    if (words.size() > 0) {
	      System.out.println("\nChecking for stopwords:");
	      for (int i = 0; i < words.size(); i++) {
	        System.out.println((i + 1) + ". " + words.get(i) + ": "
	          + stopwords.is(words.get(i).toString()));
	      }
	    }
	  }*/
	}

interface RevisionHandler {

	  /**
	   * Returns the revision string.
	   * 
	   * @return		the revision
	   */
	  public String getRevision();
	}
	 
class Revisionutils {
	  
	  /**
	   * Enumeration of source control types.
	   * 
	   * @author  fracpete (fracpete at waikato dot ac dot nz)
	   * @version $Revision: 8034 $
	   */
	  public enum Type {
	    /** unknown source control revision. */
	    UNKNOWN,
	    /** CVS. */
	    CVS,
	    /** Subversion. */
	    SUBVERSION;
	  }
	  
	  /**
	   * Extracts the revision string returned by the RevisionHandler.
	   * 
	   * @param handler	the RevisionHandler to get the revision for
	   * @return		the actual revision string
	   */
	  /*public static String extract(RevisionHandler handler) {
	    return extract(handler.getRevision());
	  }*/
	  
	  /**
	   * Extracts the revision string.
	   * 
	   * @param s		the string to get the revision string from
	   * @return		the actual revision string
	   */
	  public static String extract(String s) {
	    String	result;
	    
	    result = s;
	    result = result.replaceAll("\\$Revision:", "");
	    result = result.replaceAll("\\$", "");
	    result = result.replaceAll(" ", "");
	    
	    return result;
	  }
	  
	  /**
	   * Determines the type of a (sanitized) revision string returned by the 
	   * RevisionHandler.
	   * 
	   * @param handler	the RevisionHandler to determine the type for
	   * @return		the type, UNKNOWN if it cannot be determined
	   */
	  /*public static Type getType(RevisionHandler handler) {
	    return getType(extract(handler));
	  }*/
	  
	  /**
	   * Determines the type of a (sanitized) revision string. Use extract(String)
	   * method to extract the revision first before calling this method.
	   * 
	   * @param revision	the revision to get the type for
	   * @return		the type, UNKNOWN if it cannot be determined
	   * @see #extract(String)
	   */
	  public static Type getType(String revision) {
	    Type	result;
	    String[]	parts;
	    int		i;
	    
	    result = Type.UNKNOWN;
	    
	    // subversion?
	    try {
	      Integer.parseInt(revision);
	      result = Type.SUBVERSION;
	    }
	    catch (Exception e) {
	      // ignored
	    }
	    
	    // CVS?
	    if (result == Type.UNKNOWN) {
	      try {
		// must contain at least ONE dot
		if (revision.indexOf('.') == -1)
		  throw new Exception("invalid CVS revision - not dots!");
		
		parts = revision.split("\\.");

		// must consist of at least TWO parts/integers
		if (parts.length < 2)
		  throw new Exception("invalid CVS revision - not enough parts separated by dots!");

		// try parsing parts of revision string - must be ALL integers
		for (i = 0; i < parts.length; i++)
		  Integer.parseInt(parts[i]);
		
		result = Type.CVS;
	      }
	      catch (Exception e) {
		// ignored
	      }
	    }
	    
	    return result;
	  }
	  
	  /**
	   * For testing only. The first parameter must be a classname of a
	   * class implementing the weka.core.RevisionHandler interface.
	   * 
	   * @param args	the commandline arguments
	   * @throws Exception	if something goes wrong
	   */
	  /*public static void main(String[] args) throws Exception {
	    if (args.length != 1) {
	      System.err.println("\nUsage: " + RevisionUtils.class.getName() + " <classname>\n");
	      System.exit(1);
	    }
	  }*/
	}
	    
	   /* RevisionHandler handler = (RevisionHandler) Class.forName(args[0]).newInstance();
	    System.out.println("Type: " + getType(handler));
	    System.out.println("Revision: " + extract(handler));*/
	  


 class StopWordRemoval {

	protected static HashSet<String> m_Words = null;
	protected static Stopwords m_Swords;
	static {
	    if (m_Swords == null) {
	      m_Swords = new Stopwords();
	    }
	  }
	public void Stopwords() {
	    m_Words = new HashSet<String>();
	  //Stopwords list from Rainbow
	    
	}
	public void add(String word) {
	    if (word.trim().length() > 0)
	      m_Words.add(word.trim().toLowerCase());
	  }
	public boolean remove(String word) {
	    return m_Words.remove(word);
	  }
	public boolean is(String word) {
	    return m_Words.contains(word.toLowerCase());
	  }
	public Enumeration<String> elements() {
	    Iterator<String>    iter;
	    Vector<String>      list;

	    iter = m_Words.iterator();
	    list = new Vector<String>();

	    while (iter.hasNext())
	      list.add(iter.next());

	    // sort list
	    Collections.sort(list);

	    return list.elements();
	  }
	public String toString() {
	    Enumeration<?>   enm;
	    StringBuffer  result;

	    result = new StringBuffer();
	    enm    = elements();
	    while (enm.hasMoreElements()) {
	      result.append(enm.nextElement().toString());
	      if (enm.hasMoreElements())
	        result.append(",");
	    }

	    return result.toString();
	  }
	public static boolean isStopword(String str) {
	    return m_Swords.is(str.toLowerCase());
	  }
	Revisionutils obj=new Revisionutils();
	
	public String getRevision() {
	    return Revisionutils.extract("$Revision: 8034 $");
	  }
	
	public String StopWordRemoval_fun(String strArray) {
		String itr = new String(strArray);
		//String itr = new String("My name 090%4 is %309# Manvi and testing testing any noun any noun I am good girl what do you say ha ha ?");
		String itr2 = new String();
		String itr3 = new String();
        String itr4 = new String();
        
		itr2 = itr.replaceAll("[^a-zA-Z0-9]"," ");
		String[] args1 = itr2.split("\\s+");
		Vector<String> words = new Vector<String>();
		for (int i = 0; i < args1.length; i++) {
		if (args1[i].trim().length() > 0)
		words.add(args1[i].trim());
		}
		 Stopwords stopwords = new Stopwords();
		 int i = 0;
		 if(i == 0)
		 {
			 //System.out.println("\nStopwords:");
		 
	     // Enumeration<?> enm = stopwords.elements();
	     
	     // while (enm.hasMoreElements()) {
	    //    System.out.println((i+1) + ". " + enm.nextElement());
	    //    i++;
	     // }
	        if (words.size() > 0) {
	      //      System.out.println("\nChecking for stopwords:");
	            for ( i=0; i < words.size(); i++) {
	              if(stopwords.is(words.get(i).toString()))
	              {
	                  
	            	  itr3= itr3+' '+words.get(i).toString();
	            }
	              else 
	              {
	            	 itr4=itr4 +' '+words.get(i).toString();
	              }
	            }	           
		//System.out.println(itr3);
		///return (itr4);
	        }
	}
		return itr4;
}
	/*public static void main(String[] args)
	{
		String test = new String();
		//test=fun_test();
		
		StopWordRemoval Stop_obj = new StopWordRemoval();
		test=Stop_obj.StopWordRemoval_fun("maddy geatest test ever");
	System.out.println(test);	
	}*/
	}

public class TopFrequentWords {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: TopN <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf);
        job.setJobName("TopFrequentWords");
        job.setJarByClass(TopFrequentWords.class);
        job.setMapperClass(TopFrequentWordsMapper.class);
        //job.setCombinerClass(TopFrequentWordsReducer.class);
        job.setReducerClass(TopFrequentWordsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
      *author- Khushboo Tiwari
     */
    public static class TopFrequentWordsMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private String tokens = "[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String cleanLine = value.toString().toLowerCase().replaceAll(tokens, " ");
			StopWordRemoval obj_stop = new StopWordRemoval();
			String S_words = new String(obj_stop.StopWordRemoval_fun(cleanLine));
			S_words=obj_stop.StopWordRemoval_fun(cleanLine);
            StringTokenizer itr = new StringTokenizer(S_words);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken().trim());
                context.write(word, one);
            }
        }
    }

 
    public static class TopFrequentWordsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Map<Text, IntWritable> countMap = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            // computes the number of occurrences of a single word
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            
            countMap.put(new Text(key), new IntWritable(sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            Map<Text, IntWritable> sortedMap = sortByValues(countMap);

            int counter = 0;
            for (Text key : sortedMap.keySet()) {
                if (counter++ == 2500) {
                    break;
                }
                context.write(key, sortedMap.get(key));
            }
        }
    }

    
    public static class TopFrequentWordsCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            // computes the number of occurrences of a single word
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    /*
   * sorts the map by values. Taken from:
   * http://javarevisited.blogspot.it/2012/12/how-to-sort-hashmap-java-by-key-and-value.html
   */
    private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
        List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        //LinkedHashMap will keep the keys in the order they are inserted
        //which is currently sorted on natural ordering
        Map<K, V> sortedMap = new LinkedHashMap<K, V>();

        for (Map.Entry<K, V> entry : entries) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }

}