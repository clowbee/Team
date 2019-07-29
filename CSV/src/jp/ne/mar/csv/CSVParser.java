package jp.ne.mar.csv;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;



public class CSVParser implements Iterable<String> {
	/** Class Name */
	private static final String CLASS_NAME = CSVParser.class.getName();

	public boolean debug = false;

	/** show progress. (minutes) */
	public int progress = 0;
	private long beforeTime = 0;
	
	/** NULL OK */
	public boolean nullable = true;

	/** based row number */
    private int rownumber = 0;
    /** relative row access (default true) */
    public static boolean relativeRowAccess = true;

    /** Map file size limit: 2^20 */
    //private static final int MAP_MAX = 1048576;
    /** mapped file size */
    //private long map_file_size = 0L;
    /** mapped index */
    //private long map_file_index = 0L;
    /** mapped length */
    //private long map_length = 0L;

    /** 1 line, 1 LF */
    protected boolean line1 = false;

    /** CSV Matrix for load */
    protected List<List<String> > csvdata = null;

    /** default codepage */
    protected final static String defaultCharsetName = "UTF-8";

	// Byte
    public final static byte b_dq = '"'; //chardel
    //public final static byte b_escape = '\\';
    public byte b_coldel = ',';  // coldel
    public final static byte b_cr = '\r';
    public final static byte b_lf = '\n';
    public final static byte b_comment = '#';
    /** Unicode unknown code */
	public static final byte[] b_UNKNOWN = {(byte)0xFF, (byte)0xFD};
	
	private long max_pos = 0L;
	private int max_col = 0;
	private int max_position = 0;;
	private long csvlines = 0L;

	private Charset charset = null;
    private CharsetDecoder decoder = null;
    private InputStream stream = null;

	private Path filename = null;
	private long streamSize = 0;
    public CSVIterator csvit = null;
    private Str str = null;
    private int bisSize = 1024 * 1024 * 10; // BuffereInputStream buffer size 10Mb




	private long start = 0;
	private long end = 0;
	
	private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");


	public CSVParser () {
		clear();
	}

	public void clear() {
	    relativeRowAccess = true;
	}

    public static void main(String[] args) {
    	CSVParser.count(args);
    	try {
    		test1("main1 for", args);
    		//test2("main2 it", args);
    		//test3("main3 it", args);
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }

	public static void test1(String msg, String[] args) throws IOException {
		System.out.println("test1:" + CLASS_NAME + ":" + msg);
		String csvfile;
		csvfile = "/Users/maruyam/MyWorks/my/CDC/subscription/test7.csv";
		csvfile = "/Users/maruyam/MyWorks/my/CDC/subscription/test10.csv";
		//Path csvfile = Paths.get("/Users/maruyam/eclipse-workspace/LiveAudit/SubscriptionGenerator_args.txt");
		//String csvfile = "/Users/maruyam/MyWorks/my/CDC/subscription/test9.csv";
		//csvfile = "/Users/maruyam/MyWorks/my/CDC/subscription/YDWH011.XX210100.on.test.gz";
		//Path csvfile = Paths.get("/Users/maruyam/MyWorks/my/CDC/subscription/test8.csv");

		//InputStream stream = new MappedFileInputStream(csvfile);
		//InputStream stream = new FileInputStream(csvfile.toString());

		CSVParser csv = new CSVParser();
		byte coldel = '|';
		csv.setColdel(coldel);

	    //System.out.println("input=" + csvfile + " (" + Files.size(csvfile) + ")");
	    System.out.println("input=" + csvfile + " (" + (new File(csvfile)).length() + ")");
		//csv.debug = true;
		//csv.open(stream);
	    //csv.setNullable(false);
		csv.open(csvfile);
		csv.progress = 10;
		int num = 0;

		while (csv.hasRemaining()) {
			boolean fin = false;
			for (String str : csv) {
				num++;
				//System.out.println("check [" + str + "]");
				//if (str.length() == 0 || str.startsWith("#")) break;
				/*if (!fin && (str.length() == 0 || str.startsWith("#"))) {
					fin = true;
					continue;
				} else if (fin) {
					continue;
				}*/
				CSVIterator it = csv.csvit;
				byte[] bytes = it.getBytes();
				System.out.println(num + " --- [" + str + "] (" + dump(bytes) + ")");
			}
			//if (!fin) break;
			/*if (num % 10000000 == 0)*/ System.out.println("--------------------------" + csv.csvlines);
		}

        System.out.println("line count "  + csv.lineCount());
        csv.close();
        System.out.println(csv.information());
        
        byte[] su = {
        		(byte) 0xe3, (byte) 0x81, (byte) 0x8b,
        		(byte) 0xe3, (byte) 0x81, (byte) 0x8b, (byte) 0xe3, (byte) 0x82, (byte) 0x99
        };

        String s2 = new String(su);
    	System.out.println(s2);
        Str str = csv.new Str();
        for (int i = 0; i < su.length; i ++) {
        	System.out.println(String.format("%d - %d (0x%X)", i, str.utf8num(su[i]), su[i]));
        }
    	System.out.println("---");
        int[] i1 = str.toCodePointArray(s2);
        for (int i = 0; i < i1.length; i ++) {
        	System.out.println(String.format("%d - (0x%X)", i, su[i]));
        }
        
        /*byte[][] data = {
        		{(byte) 0xe3, (byte) 0x83, (byte) 0xbb}, // \u30Fb
        		{(byte) 0xee, (byte) 0x80, (byte) 0x9a}, // \ue01a
        		{(byte) 0xee, (byte) 0x82, (byte) 0xaf}  // \ue0af
        };
        String data1 =  new String(data[2], "UTF-8");
        System.out.println(0 + " :[" + data1 + "]");
        for (int i = 0; i < data1.length(); i++) {
        	char c = data1.charAt(i);
            System.out.println(String.format("%c 0x%x",  c, (int)c));
        }*/

       /*
       	[荢] = 1 : s1[e8 8d a2] s2[e2 96 a1]
        		[纊] = 1 : s1[e7 ba 8a] s2[e2 96 a1]
        		[擎] = 1 : s1[e6 93 8e] s2[e2 96 a1]
        		[抦] = 2 : s1[e6 8a a6] s2[e2 96 a1]
        		[汜] = 2 : s1[e6 b1 9c] s2[e2 96 a1]
        		[蓜] = 5 : s1[e8 93 9c] s2[e2 96 a1]
        		[－] = 7 : s1[e2 88 92] s2[ef bc 8d]
        		[鈹] = 8 : s1[e9 88 b9] s2[e2 96 a1]
        		[] = 2 : s1[ee 80 9a] s2[e2 96 a1]
        		[] = 5 : s1[ee 82 af] s2[e2 96 a1]*/


        /*String sss = "荢纊擎抦汜蓜－鈹";
        for (int i = 0; i < sss.length(); i++) {
        	char c = sss.charAt(i);
        	String utf8 = String.valueOf(c);

        	byte[] bEbcdic = utf8.getBytes("Cp930");
	        byte[] bUtf8 = utf8.getBytes("UTF-8");
	        System.out.print("String [" + utf8 + "] ");
	        System.out.print("UTF-8  [" + dump(bUtf8) + "] ");
	        System.out.println("EBCDIC [" + dump(bEbcdic) + "]");
        }*/
    }

	public static void test2(String msg, String[] args) throws IOException {
		System.out.println("test2:" + CLASS_NAME + ":" + msg);
		Path csvfile = Paths.get("/Users/maruyam/MyWorks/my/CDC/subscription/test7.csv");

		//InputStream stream = new MappedFileInputStream(csvfile);
		//InputStream stream = new FileInputStream(csvfile.toString());

		CSVParser csv = new CSVParser();

		System.out.println("input=" + csvfile + " (" + Files.size(csvfile) + ")");
		//csv.debug = true;
		//csv.open(stream);
		csv.open(csvfile);
		int num = 0;

		CSVIterator it;
		while (csv.hasRemaining()) {
			it = (CSVIterator)csv.iterator();
			while (it.hasNext()) {
				num++;
				String str = (String)it.next();
				byte[] bytes = it.getBytes();
				System.out.println(num + " --- [" + str + "] (" + dump(bytes) + ")");
			}
			
			/*for (String str : csv) {
				num++;
				CSVIterator it = csv.csvit;
				byte[] bytes = it.getBytes();
				System.out.println(num + " --- [" + str + "] (" + dump(bytes) + ")");
			}*/
			System.out.println("--------------------------" + csv.csvlines);
		}

        System.out.println("line count "  + csv.lineCount());
        csv.close();
        System.out.println(csv.information());
    }
	
	public static void test3(String msg, String[] args) throws IOException {
		System.out.println("test3:" + CLASS_NAME + ":" + msg);
		String filename = "/Users/maruyam/MyProjects/DM/82bank/資料/ODSレイアウト/CSV_ODSレイアウトV36_20180726/KK110500.csv";

		CSVParser csv = new CSVParser();
		csv.nullable = false;
		long linenum = csv.load(filename);
		System.out.println("line count "  + linenum);
		Map<String, int[]> tags = csv.loadTagName("TagName.txt");
		System.out.println("tags size = "  + tags.size());
		
		String tablename = csv.get(tags, "レコード定義名", 0);
		System.out.println("tablename "  + tablename);
		for (int i = 1; i <= 10; i++) {
			String biko = csv.get(tags, "備考", i);
			System.out.println(i + " :biko "  + biko);
			String columnName = csv.get(tags, "カラム名S", i);
			//System.out.println(i + " :col "  + columnName);
		}

	}

	public String information() {
    	StringBuilder sb = new StringBuilder();
    	String lf = System.getProperty("line.separator");
    	long stms = 0L;
    	if (start < end) stms = streamSize / (end - start);

        String ts1 = sdf.format(new Timestamp(start));
        String ts2 = sdf.format(new Timestamp(end));
        String fn = null;
        if (filename != null) fn = filename.getFileName().toString();

        sb.append("CSV Information -------------------- ").append(ts1).append(lf);
        sb.append("filename          = ").append(fn).append(lf);
    	sb.append("Elapsed Time      = ").append(getElapsed(start, end)).append(lf);
    	sb.append("streamSize/ms     = ").append(stms).append(lf);
    	sb.append("max col size      = ").append(max_pos).append(lf);
    	sb.append("max col num       = ").append(max_col).append(lf);
    	sb.append("max_position size = ").append(max_position).append(lf);
    	sb.append("line count        = ").append(lineCount()).append(lf);
    	sb.append("CSV Information -------------------- ").append(ts2).append(lf);

    	return sb.toString();
	}

	public byte setColdel(byte coldel) {
		byte b = this.b_coldel;
		this.b_coldel = coldel;

		return b;
	}

	public boolean setNullable(boolean nullable) {
		boolean b = this.nullable;
		this.nullable = nullable;
		return b;
	}

    public static void count(String[] args) {
    	if (args.length == 0) {
    		System.out.println("Usage: filename");
    		return;
    	}
    	String filename = args[0];
    	CSVParser csv = new CSVParser();
    	csv.progress = 10;
		try {
			//InputStream stream = new FileInputStream(filename);
			//InputStream stream = new BufferedInputStream(new FileInputStream(filename));
			//InputStream stream = new MappedFileInputStream(Paths.get(filename));
			//csv.open(stream);
			csv.open(filename);
			while (csv.hasRemaining()) {
				for (String s : csv) {
					break;
				}
			}
	        csv.close();
	        System.out.println("line count = "  + csv.lineCount());
	        System.out.println(csv.information());
		} catch (IOException e) {
			e.printStackTrace();
		}
    }

    public static String dump(String str) {
		if (str == null) return null;
    	byte[] bytes = str.getBytes();
		return dump(bytes, 0, bytes.length);
	}

    public static String dump(byte[] bytes) {
    	if (bytes == null) return null;
        return dump(bytes, 0, bytes.length);
    }

    public static String dump(byte[] bytes, int offset, int length) {
		if (bytes == null) return null;
		StringBuilder sb = new StringBuilder();

		for (int i = offset; i < offset + length; i++) {
			if (i > offset) sb.append(" ");
			sb.append(String.format("%02x", bytes[i]));
		}
		return sb.toString();
	}

    public static String dump(ByteBuffer byteBuffer) {
		if (byteBuffer == null) return null;

		StringBuilder sb = new StringBuilder();
	    //System.out.println("dump [" + dump(bb.array(), 0, bb.limit()) + "]");
		for (int i = 0; i < byteBuffer.limit(); i++) {
			if (i > 0) sb.append(" ");
			sb.append(String.format("%02x", byteBuffer.get()));
		}
		return sb.toString();
	}

    public class CSVIterator implements java.util.Iterator<String> {
        private CharsetDecoder decoder = null;
        private CharBuffer charBuffer = null;
        private boolean lineEnd = true;
        private Str str = null;

        public CSVIterator(CharsetDecoder decoder, Str instr) {
        	this.decoder = decoder;
        	this.str = instr;

            //if (instr == null) str = new Str();
            str.clear();
            str.state = State.nonescaped;
        	lineEnd = hasRemaining(); // TODO
        }

        public boolean hasNext() {
        	return lineEnd;
        }

        public String next() {
			StopState result;
        	str.stopState = StopState.none;

        	try {
        		str.clear();
        		result = getColumn(str); // TODO
        		if (str.stopState == StopState.lf) {
        			lineEnd = false;
        			str.coln = 0;
        			str.pos = 0;
        		}
        		if (str.isNull) {
        			str.str = null;
        		} else {
        			str.toString();
        		}
			} catch (IOException e1) {
				e1.printStackTrace();
			}

        	return str.toString();
        }

        public ByteBuffer getByteBuffer() {
        	if (str == null) return null;
        	return str.byteBuffer;
        }
        
        public void setDecoder(CharsetDecoder decoder, CharBuffer charBuffer) {
        	this.decoder = decoder;
        	this.charBuffer = charBuffer;
        }

        public byte[] getBytes() {
        	if (str == null) return null;
        	return str.getBytes();
        }

        public void remove() {
            /* n/a */
        }
    }

    public java.util.Iterator<String> iterator() {
    	if (csvit != null && csvit.hasNext()) {
    		while (csvit.hasNext()) {
    			csvit.next();
    		}
    	}
    	csvit = new CSVIterator(this.decoder, this.str);
    	return csvit;
    }
    
    public void open(String filename) throws IOException {
		open(Paths.get(filename));
    }

    public void open(Path path) throws IOException {
		InputStream stream;
		if (path.toString().endsWith(".gz")) {
			stream = new BufferedInputStream(new GZIPInputStream(new MappedFileInputStream(path)), bisSize);
			//stream = new BufferedInputStream(new GzipInputStream(path), bisSize);
			//stream = new BufferedInputStream(new GZIPInputStream(new DebugFileInputStream(path)), bisSize);
    		//stream = new BufferedInputStream(new GZIPInputStream(new FileInputStream(new File(path.toString()))), bisSize);
    	} else {
    		stream = new MappedFileInputStream(path);
    	}
		filename = path;
		open(stream);
    }

    public void open(InputStream stream) throws IOException {
    	if (stream == null) throw new IOException("stream not found.");
    	if (this.stream != null) this.stream.close();
    	this.stream = stream;

		if (charset == null) charset = Charset.forName(defaultCharsetName);
		if (decoder == null) decoder = charset.newDecoder().onMalformedInput(CodingErrorAction.IGNORE).onUnmappableCharacter(CodingErrorAction.IGNORE);
		relativeRowAccess = true;
		str = new Str();

		// Statistics
		max_pos = 0L;
		max_col = 0;
		max_position = 0;;
		csvlines = 0L;
		start = System.currentTimeMillis();
		beforeTime = start;
		
        //bo = new BufferedOutputStream(new FileOutputStream("/Users/maruyam/MyWorks/my/CDC/subscription/binary.dat"));

	}

    public void close() {
    	end = System.currentTimeMillis();
    	try {
    		if (stream != null) stream.close();
    	} catch (Exception e) {}

    	charset = null;
        decoder = null;
		//streamSize = -1;
	}

    public boolean hasRemaining() {
    	try {
			return stream.available() > 0;
		} catch (IOException e) {
		}
    	return false;
    }

    public byte getByte() throws IOException {
    	streamSize++;

    	if (!hasRemaining()) throw new IOException("no remaining.");

		return (byte) stream.read();
    }

    /**
     * only one line
     * @param line1 flag
     * @return old option
     */
    public boolean line1(boolean line1) {
    	boolean t = this.line1;
    	this.line1 = line1;

    	return t;
    }

    public boolean line1() {
    	return line1;
    }

    public enum State {
    	none,
    	nonescaped,
    	afterCR,
    	escaped,
    	afterDQ
    }

    public enum StopState {
    	none,
    	success,
    	coldel,
    	lf,
    	getnull
    }

    public class Str {
    	private ByteBuffer byteBuffer = null;
    	private StopState stopState = StopState.none;
    	public State state = State.nonescaped;
    	private byte[] bytes = null;
    	/** column number */
    	public int coln = 0;
        /** line position */
        public int pos = 0;
        private CharsetDecoder decoder = null;
        private CharBuffer charBuffer = null;
        public boolean isNull = false;
        private String str = null;

        public Str() {
            decoder = newDecoder(defaultCharsetName, CodingErrorAction.IGNORE);
            byteBuffer = ByteBuffer.allocate(1024);
            charBuffer = CharBuffer.allocate(1024);
            state = State.nonescaped;
        }

        public Str(CharsetDecoder decoder, CharBuffer charBuffer) {
        	this.charBuffer = charBuffer;
        	this.decoder = decoder;
            if (decoder == null ) this.decoder = newDecoder(defaultCharsetName, CodingErrorAction.IGNORE);
            if (byteBuffer == null ) this.byteBuffer = ByteBuffer.allocate(1024);
            if (charBuffer == null ) this.charBuffer = CharBuffer.allocate(1024);
            state = State.nonescaped;
        }

        public ByteBuffer allocate(int size) {
        	byteBuffer = ByteBuffer.allocate(size);

        	return byteBuffer;
        }

        public ByteBuffer put(byte b) {
        	if (byteBuffer == null || byteBuffer.position() == byteBuffer.capacity()) {
        		int size = 11;
        		if (byteBuffer != null) size = byteBuffer.capacity();
        		ByteBuffer replace = ByteBuffer.allocate(size * 2);
        		if (byteBuffer != null) replace.put(byteBuffer);
        		byteBuffer = replace;
        	}
        	
        	/* statistic */
        	max_position = Math.max(max_position,  byteBuffer.position());

        	byteBuffer.put(b);
            isNull = false;


        	return byteBuffer;
        }
		public void flip() {
			if (byteBuffer == null) return;
			byteBuffer.flip();
		}
        
		public int position() {
			if (byteBuffer == null) return 0;
			return byteBuffer.position();
		}

		public final ByteBuffer position(int position) {
			byteBuffer.position(position);
			return byteBuffer;
		}

		public int limit() {
			if (byteBuffer == null) return 0;
			return byteBuffer.limit();
		}

		public final ByteBuffer limit(int limit) {
			byteBuffer.limit(limit);
			return byteBuffer;
		}

		public int size() {
        	if (byteBuffer == null) return 0;
        	return byteBuffer.limit() - byteBuffer.position();
        }

		/*
	     * サロゲート
	     *  var a;  //上位サロゲート
	     *  var b;  //下位サロゲート
	     *  x = 0x20B9F;  //文字コードをセット
	     *  x -= 0x10000;
	     *  a = Math.floor(x / 0x400);  //Math.floor()で整数値に変換
	     *  a += 0xD800;
	     *  b = x % 0x400;
	     *  b += 0xDC00;
	     *  上位サロゲートは、U+D800からU+DBFFの範囲にあります。下位サロゲートは、U+DC00からU+DFFFまでの範囲にあります。
	     * Unicodeの外字範囲をチェックする。
	     *  BMP領域 U+E000‐U+F8FF (6,400字)
	     *  15面 U+F0000‐U+FFFFD (65,534字)
	     *  16面 U+100000‐U+10FFFD (65,534字)
	     *  余談：シフトJISの外字範囲：U+E000～U+E757（0xF040～0xF9FC）。
	     * 結合文字
	     */
        public int utf8num(byte b) {
        	int keta = 0;
        	if ((byte)0x00 <= b && b <= (byte)0x7f) {
    			keta = 1;
    		} else if ((byte)0xc2 <= b && b <= (byte)0xdf) {
    			keta = 2;
    		} else if ((byte)0xe0 <= b && b <= (byte)0xef) {
    			keta = 3;
    		} else if ((byte)0xf0 <= b && b <= (byte)0xff) {
    			keta = 4;
    		}
        	return keta;
        }

        public CharsetDecoder newDecoder(final String charsetName) {
        	return newDecoder(charsetName, CodingErrorAction.IGNORE);
        }

        public CharsetDecoder newDecoder(final String charsetName, CodingErrorAction action) {
        	charset = Charset.forName(charsetName);

			// set new decoder
			//decoder = charset.newDecoder().onMalformedInput(CodingErrorAction.IGNORE);
			//decoder = charset.newDecoder().onMalformedInput(CodingErrorAction.REPLACE).onUnmappableCharacter(CodingErrorAction.REPLACE);
	    	////////decoder = charset.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT);
			//charBuffer = decoder.onMalformedInput(CodingErrorAction.IGNORE).decode(mbb);
			//decoder = charset.newDecoder().onUnmappableCharacter(CodingErrorAction.REPLACE);
			//decoder = charset.newDecoder().onUnmappableCharacter(CodingErrorAction.REPORT);
	
	    	//decoder = charset.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT);
	    	decoder = charset.newDecoder().onMalformedInput(action).onUnmappableCharacter(action);
	    	//System.out.println("[" + decoder.replacement() + "]");
	    	//decoder.replaceWith("?");

	    	return decoder;
        }

        public int decode(CharsetDecoder decoder, ByteBuffer byteBuffer, CharBuffer charBuffer) {
            return decode(decoder, byteBuffer, charBuffer, true);
        }

        public int decode(CharsetDecoder decoder, ByteBuffer byteBuffer, CharBuffer charBuffer, boolean endOfInput) {
        	CoderResult corderResult = null;
        	int rc = 0;
    		//System.out.println("byteBuf[pos=" + byteBuffer.position() + " lim=" + byteBuffer.limit() + " cap=" + byteBuffer.capacity() + "]");
    		//System.out.println("CharBuf[pos=" + charBuffer.position() + " lim=" + charBuffer.limit() + " cap=" + charBuffer.capacity() + "]");

        	for (;;) {
    	    	corderResult = decoder.decode(byteBuffer, charBuffer, endOfInput); // no more data
    	    	if (!corderResult.isError()) break;
    	    	rc++;
        		if (corderResult.isMalformed()) {
        			if (debug) {
        				System.err.println("Malformed");
    					//System.out.println("CharBuf[pos=" + charBuf.position() + " lim=" + charBuf.limit() + " cap=" + charBuf.capacity() + "]");
    					System.err.println("mbb[pos=" + byteBuffer.position() + ", " + corderResult.length() + "]");
        			}
    				for (int i = 0; i < corderResult.length(); i++) {
    					byte b = byteBuffer.get();// corderResult.length()
    					if (debug) System.err.println(String.format("%d:mbb=0x%X (%d)", i, b, b));
    				}
    				//System.err.println("point = " + charBuffer.position());
    	            //charBuffer.put(decoder.replacement());
    			}
    			if (corderResult.isUnmappable()) {
    				if (debug) System.err.println("Unmappable");
    			}
        	}
    		charBuffer.flip();

        	return rc;
        }

        public boolean isSurrogatePair(char c1, char c2) {
        	if ('\uD800' <= c1 && c1 <= '\uDBFF') {
                if ('\uDC00' <= c2 && c2 <= '\uDFFF') {
                	return true;
                }
            }
        	return false;
        }

        public boolean isCombiningCharacter() {
        	// not implemented
        	return false;
        }

        int[] toCodePointArray(String str) {
        	char[] ach = str.toCharArray(); // a char array copied from str
        	int len = ach.length;           // the length of ach
        	int[] acp = new int[Character.codePointCount(ach, 0, len)];
        	int j = 0;                      // an index for acp

        	for (int i = 0, cp; i < len; i += Character.charCount(cp)) {
        		cp = Character.codePointAt(ach, i);
        		acp[j++] = cp;
        	}
        	return acp;
        }

		public StopState result(StopState result) {
			this.stopState = result;
			byteBuffer.flip();
			return result;
		}

		public void clear() {
	    	byteBuffer.clear();
	    	stopState = StopState.none;
	    	//state = State.nonescaped;
	    	bytes = null;
	    	//coln = 0;
	        //pos = 0;
	        //decoder = null;
	        charBuffer.clear();
	        isNull = false;
	        //isNullString = false;
	        str = null;
		}

		public String toString() {
			//System.out.println("toString:" + byteBuffer);
			if (byteBuffer == null || isNull) {
				str = null;
				return null;
			}

			if (str != null) return str;

			charBuffer.clear();
			int limit = byteBuffer.limit();
			int position = byteBuffer.position();
			//byteBuffer.mark();
			//byteBuffer.flip();
			decode(decoder, byteBuffer, charBuffer);
			byteBuffer.limit(limit);
			byteBuffer.position(position);
			//byteBuffer.reset();

			str = charBuffer.toString();
			return str;
		}

		public byte[] getBytes() {
        	//if (isNull || isNullString) return null;
        	if (isNull) return null;
        	//if (limit() == 0) return null;
        	int limit = byteBuffer.limit();
        	int position = byteBuffer.position();
        	//byteBuffer.flip();
        	//System.out.println(position + "-" + limit);
        	byte[] bytes = new byte[byteBuffer.limit()];
        	byteBuffer.get(bytes, 0, byteBuffer.limit());
        	//System.out.println(str.byteBuffer);
        	byteBuffer.limit(limit);
        	byteBuffer.position(position);

        	return bytes;
        }

		public StopState setStopState(StopState state) {
			StopState result = this.stopState;
			this.stopState = state;
			
			return result;
		}

		public StopState setStopState(StopState state, boolean isNull) {
			StopState result = this.stopState;
			this.stopState = state;
			this.isNull = isNull;
			
			return result;
		}
    }

    public StopState getColumn(Str str) throws IOException {
    	if (str == null) throw new IOException("Str is null.");
        //State state = State.nonescaped;
        byte ch = 0;
        //int pos = 0;
        int utf8n = 0; // UTF8文字数
        int utf8c = 0; // UTF8カウントダウン

        while (hasRemaining()) {
        	str.pos++;
        	ch = getByte();
        	if (utf8c == 0) {
        		utf8n = str.utf8num(ch);
        		utf8c = utf8n;
        	}

    		if (debug) System.out.println(String.format("%d-%d: getByte -> %d (%02X) %s %d-%d", str.size(), str.pos, ch, ch, str.state, utf8n, utf8c));
        	/* debug fast
        	if (utf8n != utf8c && str.state != State.escaped) {
        		if (ch != b_coldel) {
            		str.put(ch);
                	utf8c--;
            		continue;
        		}
        	}*/
        	utf8c--;
        	if (str.state == State.nonescaped) {
        		if (ch == b_cr) {
        			// non
        			str.state = State.afterCR;
        		} else if (ch == b_lf) {
        			// append, NULL
        			//bb = appendbb1(bb, ch);
        			//str.put(b_lf);
            		if (str.position() == 0) {
                		if (nullable) {
                			//cols.add(null);
                			str.isNull = true;
                		}
            			str.limit(0);
            		}
        			str.setStopState(StopState.lf);
        	        csvlines++;
        			break;
        		} else if (ch == b_dq) {
        			// non
        			str.state = State.escaped;
        		} else if (ch == b_coldel) {
        			// field add, NULL
            		if (str.position() == 0) {
                		if (nullable) {
                			//cols.add(null);
                			str.isNull = true;
                		}
            			str.limit(0);
            		}
        			str.setStopState(StopState.coldel);
        			break; // add break
            		//System.out.println("--------------------------------------------");
        		} else {
        			// append
        			//bb = appendbb1(bb, ch);
            		str.put(ch);
        		}
        	} else if (str.state == State.afterCR) {
        		if (ch == b_cr) {
        			// append
        			//bb = appendbb1(bb, ch);
            		str.put(b_cr);
        		} else if (ch == b_lf) {
        			// field add, record break
        			//bb.flip();
        			//cols.add(bb);
        			str.state = State.nonescaped;
        			//System.out.println("--------------------------------------------");
        			str.setStopState(StopState.lf);
        	        csvlines++;

        			break;
        		} else if (ch == b_dq) {
        			//append cr
        			//bb = appendbb1(bb, b_cr);
            		str.put(b_cr);
            		str.state = State.escaped;
        		} else if (ch == b_coldel) {
        			// append cr, field add
        			//bb = appendbb1(bb, b_cr);
            		str.put(b_cr);
        			//bb.flip();
        			//cols.add(bb);
            		//System.out.println("--------------------------------------------");
        			str.state = State.nonescaped;
        			str.setStopState(StopState.coldel);
        			break; // add break
        		} else {
        			// append cr, append ch
        			//bb = appendbb1(bb, b_cr);
        			//bb = appendbb1(bb, ch);
            		str.put(b_cr);
            		str.put(ch);
            		str.state = State.nonescaped;
        		}
        	} else if (str.state == State.escaped) {
        		if (ch == b_cr) {
        			// append
        			//bb = appendbb1(bb, ch);
            		str.put(ch);
        		} else if (ch == b_lf) {
        			// append
        			//bb = appendbb1(bb, ch);
            		str.put(b_lf);
        			if (line1) {
            			str.setStopState(StopState.lf);
            	        csvlines++;
        				break;
        			}
        		} else if (ch == b_dq) {
        			// non
        			str.state = State.afterDQ;
        		} else if (ch == b_coldel) {
        			// append
        			//bb = appendbb1(bb, ch);
        			str.put(b_coldel);
        		} else {
        			// append
        			//bb = appendbb1(bb, ch);
            		str.put(ch);
        		}
        	} else if (str.state == State.afterDQ) {
        		if (ch == b_cr) {
        			// non
        			str.state = State.afterCR;
        		} else if (ch == b_lf) {
        			// append
        			//bb = appendbb1(bb, ch); // null strings
        			//bb.flip();
        			//cols.add(bb);
        			if (str.position() == 0) {
            			str.limit(0);
            		}

        			str.state = State.nonescaped;
        			str.setStopState(StopState.lf);
        	        csvlines++;
        			break;
        		} else if (ch == b_dq) {
        			// append
        			//bb = appendbb1(bb, b_dq);
        			str.put(b_dq);
        			str.state = State.escaped;
        		} else if (ch == b_coldel) {
        			// field add, null strings
        			//bb.flip();
        			//cols.add(bb);
            		//System.out.println("--------------------------------------------");

        			str.state = State.nonescaped;
            		if (nullable && str.position() == 0) {
            			//cols.add(null);
            			str.limit(0);
            		}
        			str.setStopState(StopState.coldel);
        			break; // add break
        		} else {
        			// append
        			//bb = appendbb1(bb, ch);
            		str.put(ch);
            		str.state = State.nonescaped; //->
        			//?state = State.escaped;
        		}
    		} else {
    			System.err.println("else byte");
    		}
        }

        if (str.position() > 0) {
			str.flip();
			//cols.add(bb);
			//str.setStopState(StopState.lf);
        }

        // show progress status
    	long t = System.currentTimeMillis();
    	if (progress > 0 && (t - beforeTime) > progress * 1000 * 60) {
    		System.out.println("in progress... " + sdf.format(new Timestamp(t)) + " " + csvlines);
    		beforeTime = System.currentTimeMillis();
    	}

		str.coln++;
    	max_col = Math.max(max_col, str.coln);
    	max_pos = Math.max(max_pos, str.pos);

        if (str.stopState == StopState.none) return str.result(StopState.lf); 
        return str.stopState;
    }

    public long lineCount() {
		return csvlines;
	}


    // Utility ---------------------------------------------------------------------
    // Class clazz = Class.forName("jp.co.abc.sample.Cdc");
    // Object obj = clazz.newInstance();
    // System.out.println(obj.getClass());

    public long load(String path) throws IOException {
    	return load(Paths.get(path), defaultCharsetName);
    }

    public long load(String path, String charName) throws IOException {
    	return load(Paths.get(path), charName);
    }

    public long load(Path path) throws IOException {
    	return load(path, defaultCharsetName);
    }

    /**
     * CSV Load Utility
     * @param path
     * @param charName
     * @return load line number
     * @throws IOException
     */
    public long load(Path path, String charName) throws IOException {
    	long linenum = 0L;
        open(path);

        try {
	        csvdata = new ArrayList<List<String> >();
	
			while (hasRemaining()) {
	            ArrayList<String> arr = new ArrayList<String>();
	            for (String s : this) {
	            	//System.out.println("[" + s + "]");
	            	arr.add(s);
	            }
	
	            //if (arr.size() > 0) csvdata.add(arr);
	            csvdata.add(arr);
	
	            linenum++;	
			}
			//close();
        } catch (OutOfMemoryError e) {
        	throw new OutOfMemoryError("Lines = " + linenum);
        }

		return linenum;
    }

    /**
     * CSV Load Utility (Tag Name)
     * @param filename
     * @return
     * @throws IOException
     */
    public Map<String, int[]> loadTagName(String filename) throws IOException {
        Map<String, int[]> tags = new HashMap<String, int[]>();
        String key;
    	int[] pos;

    	try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filename), StandardCharsets.UTF_8));){
            String str;
            while ((str = reader.readLine()) != null) {
            	if (str == null || str.startsWith("#")) continue;
            	if (str.startsWith("$LABEL_ROW")) {
            		int ind = str.indexOf("=");
            		if (ind < 0) continue;
            		rownumber = Integer.parseInt(str.substring(ind + 1).trim());
            		List<String> list = csvdata.get(rownumber);
            		int n = 0;
            		for (String s : list) {
            			if (s != null && !"".equals(s)) {
	            			pos = new int[2];
	            			pos[0] = rownumber;
	            			pos[1] = n;
	                    	//String sss = s.replaceAll(" ", "").replaceAll("　", "");
	                    	tags.put(s.replaceAll(" ", "").replaceAll("　", ""), pos);
	                    	//System.out.println("put:: [" + sss + "]");
            			}
                    	n++;
            		}
            	} else {
	            	String[] keys = str.split(",");
	            	if (keys.length < 3) {
	            		//System.out.println("error=[" + str + "]");
	            		continue;
	            	}
	            	pos = new int[2];
	            	key = keys[0];
	            	pos[0] = Integer.parseInt(keys[1]);
	            	pos[1] = Integer.parseInt(keys[2]);
	            	tags.put(key, pos);
            	}
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    	return tags;
    }

    public int getInt(Map<String, int[]> tags, String tag, int row) throws NumberFormatException {
    	String s = get(tags, tag, row);
    	if (s == null || "".equals(s)) return 0;

    	return Integer.parseInt(s);
    }

    public String get(Map<String, int[]> tags, String tag, int row) {
    	int[] key = tags.get(tag);
    	if (key == null) {
    		//System.err.println("get:: tag not found. ('" + tag + "')");
    		return null;
    	}
    	if (key.length == 0) return null;
    	//System.out.println("key=" + key[0] + ", " + key[1]);

    	if (relativeRowAccess) row += key[0];

    	return get(row, key[1]);
    }
    
    public int getInt(int row, int col) throws NumberFormatException {
    	String s = get(row, col);
    	if (s == null || "".equals(s)) return 0;

    	return Integer.parseInt(s);
    }

    public String get(int row, int col) {
    	//System.out.println("max row=" + row + ":" + csvdata.size() + "/" + col + ":" + csvdata.get(row).size());
    	if (csvdata.size() <= row) return null;
    	if (csvdata.get(row).size() <= col) return null;

    	//System.out.println("get:: = " + row + ":" + col + " = [" + csvdata.get(row).get(col) + "]");
    	return csvdata.get(row).get(col);
    }

    public int getMaxRows() {
    	return getMaxRows(this.relativeRowAccess);
    }

    /**
     * csvdata
     * @param relativeRowAccess
     * @return
     */
    public int getMaxRows(boolean relativeRowAccess) {
    	if (csvdata == null) return 0;
    	if (relativeRowAccess) {
    		return csvdata.size() - rownumber;
    	}
    	return csvdata.size();
    }
    
    public static String getElapsed(long aInitialTime, long aEndTime) {
		StringBuilder elapsed = new StringBuilder();
		long milliseconds = aEndTime - aInitialTime;

		long seconds = milliseconds / 1000;
		long minutes = milliseconds / (60 * 1000);
		long hours = milliseconds / (60 * 60 * 1000);
		long days = milliseconds / (24 * 60 * 60 * 1000);

		if (days > 0) {
			hours = hours % 24;
			elapsed.append(days).append(" days ");
		}

		if (hours > 0) {
			minutes = minutes % 60;
			elapsed.append(hours).append(" hours ");
		}

		if (minutes > 0) {
			seconds = seconds % 60;
			elapsed.append(minutes).append(" minutes ");
		}

		if (seconds > 0) {
			elapsed.append(seconds).append(" seconds ");
		}
		milliseconds = milliseconds % 1000;
		elapsed.append(milliseconds).append(" ms.");

		return elapsed.toString();
	}
}