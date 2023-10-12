/**
 * sql.flex - JFlex specification for a lexical analyzer for
 * individual SQL statements from the subset of SQL supported by our parser
 *
 * Portions of this file are based on the examples in the
 * JFlex distribution.
 */

import java_cup.runtime.*;

public class Lexer implements java_cup.runtime.Scanner {
    
    /** This character denotes the end of file */
    final public static int YYEOF = -1;
    
    /** initial size of the lookahead buffer */
    final private static int YY_BUFFERSIZE = 16384;
    
    /** lexical states */
    final public static int YYINITIAL = 0;
    
    /** 
     * Translates characters to character classes
     */
    final private static String yycmap_packed = 
        "\10\0\3\5\25\0\1\5\1\44\1\45\4\0\1\46\1\34\1\35"+
        "\1\37\1\3\1\36\1\3\1\4\1\0\12\2\1\0\1\40\1\43"+
        "\1\41\1\42\2\0\1\6\1\13\1\17\1\11\1\14\1\27\1\15"+
        "\1\20\1\16\1\1\1\22\1\7\1\24\1\10\1\23\1\26\1\1"+
        "\1\21\1\12\1\25\1\31\1\32\1\33\1\1\1\30\1\1\6\0"+
        "\1\6\1\13\1\17\1\11\1\14\1\27\1\15\1\20\1\16\1\1"+
        "\1\22\1\7\1\24\1\10\1\23\1\26\1\1\1\21\1\12\1\25"+
        "\1\31\1\32\1\33\1\1\1\30\1\1\uff85\0";
    
    /** 
     * Translates characters to character classes
     */
    final private static char [] yycmap = yy_unpack_cmap(yycmap_packed);
    
    /** 
     * Translates a state to a row index in the transition table
     */
    final private static int yy_rowMap [] = { 
        0,    39,    78,   117,   156,    39,    39,   195,   234,   273, 
        312,   351,   390,   429,   468,   507,   546,   585,   624,   663, 
        702,   741,   780,   819,    39,    39,    39,    39,    39,   858, 
        897,   936,   975,  1014,  1053,  1092,  1131,  1170,  1209,    78, 
        1248,  1287,  1326,  1365,  1404,  1443,  1482,  1521,  1560,    78, 
        1599,  1638,  1677,  1716,  1755,  1794,  1833,    78,  1872,  1911, 
        1950,  1989,  2028,  2067,  2106,    39,    39,    39,    39,  1014, 
        39,  1053,  1092,    78,    78,  2145,  2184,    78,  2223,  2262, 
        2301,  2340,  2379,    78,  2418,  2457,  2496,  2535,  2574,  2613, 
        2652,  2691,  2730,    78,  2769,  2808,  2847,  2886,  2925,  2964, 
        3003,  3042,    78,  3081,    78,  3120,  3159,    78,  3198,  3237, 
        3276,  3315,    78,  3354,    78,  3393,  3432,    78,  3471,  3510, 
        3549,    78,  3588,  3627,  3666,  3705,    78,    78,  3744,  3783, 
        3822,    78,  3861,  3900,    78,  3939,  3978,  4017,    78,  4056, 
        4095,  4134,  4173,    78,    78,  4212,    78,    78,  4251,    78, 
        78,  4290,  4329,    78,    78,  4368,  4407,    78,  4446,    78, 
        78,    78,    78
    };
    
    /** 
     * The packed transition table of the DFA (part 0)
     */
    final private static String yy_packed0 = 
        "\1\2\1\3\1\4\1\5\1\6\1\7\1\10\1\11"+
        "\1\12\1\13\1\14\1\15\2\3\1\16\1\17\1\3"+
        "\1\20\1\21\1\22\1\3\1\23\1\24\1\25\1\3"+
        "\1\26\1\27\1\30\1\31\1\32\1\33\1\34\1\35"+
        "\1\36\1\37\1\40\1\41\1\42\1\43\50\0\2\3"+
        "\3\0\26\3\14\0\1\3\1\4\1\0\1\44\1\0"+
        "\26\3\15\0\1\45\45\0\2\3\3\0\1\3\1\46"+
        "\1\47\1\3\1\50\21\3\14\0\2\3\3\0\10\3"+
        "\1\51\15\3\14\0\2\3\3\0\15\3\1\52\5\3"+
        "\1\53\2\3\14\0\2\3\3\0\6\3\1\54\1\3"+
        "\1\55\2\3\1\56\12\3\14\0\2\3\3\0\6\3"+
        "\1\57\17\3\14\0\2\3\3\0\6\3\1\60\17\3"+
        "\14\0\2\3\3\0\2\3\1\61\1\3\1\62\21\3"+
        "\14\0\2\3\3\0\1\3\1\63\10\3\1\64\1\65"+
        "\1\3\1\66\10\3\14\0\2\3\3\0\6\3\1\67"+
        "\6\3\1\70\10\3\14\0\2\3\3\0\6\3\1\71"+
        "\17\3\14\0\2\3\3\0\13\3\1\72\12\3\14\0"+
        "\2\3\3\0\1\73\25\3\14\0\2\3\3\0\13\3"+
        "\1\74\12\3\14\0\2\3\3\0\13\3\1\75\12\3"+
        "\14\0\2\3\3\0\20\3\1\76\5\3\14\0\2\3"+
        "\3\0\1\77\25\3\14\0\2\3\3\0\12\3\1\100"+
        "\2\3\1\101\10\3\54\0\1\102\46\0\1\103\46\0"+
        "\1\104\1\105\45\0\1\105\5\0\45\106\1\107\1\106"+
        "\46\110\1\107\2\0\1\111\46\0\1\45\1\0\1\44"+
        "\43\0\2\3\3\0\1\3\1\112\24\3\14\0\2\3"+
        "\3\0\3\3\1\113\22\3\14\0\2\3\3\0\14\3"+
        "\1\114\1\3\1\115\7\3\14\0\2\3\3\0\17\3"+
        "\1\116\6\3\14\0\2\3\3\0\1\3\1\117\24\3"+
        "\14\0\2\3\3\0\1\3\1\120\24\3\14\0\2\3"+
        "\3\0\4\3\1\121\21\3\14\0\2\3\3\0\15\3"+
        "\1\122\10\3\14\0\2\3\3\0\1\3\1\123\15\3"+
        "\1\124\6\3\14\0\2\3\3\0\7\3\1\125\16\3"+
        "\14\0\2\3\3\0\4\3\1\126\12\3\1\127\6\3"+
        "\14\0\2\3\3\0\10\3\1\130\15\3\14\0\2\3"+
        "\3\0\1\131\25\3\14\0\2\3\3\0\6\3\1\132"+
        "\17\3\14\0\2\3\3\0\16\3\1\133\7\3\14\0"+
        "\2\3\3\0\1\134\25\3\14\0\2\3\3\0\1\3"+
        "\1\135\24\3\14\0\2\3\3\0\22\3\1\136\3\3"+
        "\14\0\2\3\3\0\5\3\1\137\20\3\14\0\2\3"+
        "\3\0\10\3\1\140\15\3\14\0\2\3\3\0\15\3"+
        "\1\141\10\3\14\0\2\3\3\0\3\3\1\142\22\3"+
        "\14\0\2\3\3\0\1\3\1\143\11\3\1\144\12\3"+
        "\14\0\2\3\3\0\6\3\1\145\17\3\14\0\2\3"+
        "\3\0\13\3\1\146\12\3\14\0\2\3\3\0\6\3"+
        "\1\147\17\3\14\0\2\3\3\0\10\3\1\150\15\3"+
        "\14\0\2\3\3\0\1\3\1\151\24\3\14\0\2\3"+
        "\3\0\6\3\1\152\17\3\14\0\2\3\3\0\17\3"+
        "\1\153\6\3\14\0\2\3\3\0\20\3\1\154\5\3"+
        "\14\0\2\3\3\0\6\3\1\155\17\3\14\0\2\3"+
        "\3\0\10\3\1\156\15\3\14\0\2\3\3\0\6\3"+
        "\1\157\17\3\14\0\2\3\3\0\6\3\1\160\6\3"+
        "\1\161\10\3\14\0\2\3\3\0\14\3\1\162\11\3"+
        "\14\0\2\3\3\0\13\3\1\163\12\3\14\0\2\3"+
        "\3\0\1\164\25\3\14\0\2\3\3\0\16\3\1\165"+
        "\7\3\14\0\2\3\3\0\1\3\1\166\24\3\14\0"+
        "\2\3\3\0\1\3\1\167\24\3\14\0\2\3\3\0"+
        "\1\3\1\170\24\3\14\0\2\3\3\0\16\3\1\171"+
        "\7\3\14\0\2\3\3\0\16\3\1\172\7\3\14\0"+
        "\2\3\3\0\1\173\25\3\14\0\2\3\3\0\23\3"+
        "\1\174\2\3\14\0\2\3\3\0\11\3\1\175\14\3"+
        "\14\0\2\3\3\0\13\3\1\176\12\3\14\0\2\3"+
        "\3\0\14\3\1\177\11\3\14\0\2\3\3\0\17\3"+
        "\1\200\6\3\14\0\2\3\3\0\17\3\1\201\6\3"+
        "\14\0\2\3\3\0\10\3\1\202\15\3\14\0\2\3"+
        "\3\0\11\3\1\203\14\3\14\0\2\3\3\0\2\3"+
        "\1\204\23\3\14\0\2\3\3\0\13\3\1\205\12\3"+
        "\14\0\2\3\3\0\7\3\1\206\16\3\14\0\2\3"+
        "\3\0\6\3\1\207\17\3\14\0\2\3\3\0\17\3"+
        "\1\210\6\3\14\0\2\3\3\0\10\3\1\211\15\3"+
        "\14\0\2\3\3\0\5\3\1\212\20\3\14\0\2\3"+
        "\3\0\6\3\1\213\17\3\14\0\2\3\3\0\1\214"+
        "\25\3\14\0\2\3\3\0\17\3\1\215\6\3\14\0"+
        "\2\3\3\0\6\3\1\216\17\3\14\0\2\3\3\0"+
        "\12\3\1\217\13\3\14\0\2\3\3\0\6\3\1\220"+
        "\17\3\14\0\2\3\3\0\6\3\1\221\17\3\14\0"+
        "\2\3\3\0\2\3\1\222\23\3\14\0\2\3\3\0"+
        "\17\3\1\223\6\3\14\0\2\3\3\0\17\3\1\224"+
        "\6\3\14\0\2\3\3\0\6\3\1\225\17\3\14\0"+
        "\2\3\3\0\6\3\1\226\17\3\14\0\2\3\3\0"+
        "\17\3\1\227\6\3\14\0\2\3\3\0\1\230\25\3"+
        "\14\0\2\3\3\0\13\3\1\231\12\3\14\0\2\3"+
        "\3\0\6\3\1\232\17\3\14\0\2\3\3\0\4\3"+
        "\1\233\21\3\14\0\2\3\3\0\1\234\25\3\14\0"+
        "\2\3\3\0\11\3\1\235\14\3\14\0\2\3\3\0"+
        "\13\3\1\236\12\3\14\0\2\3\3\0\11\3\1\237"+
        "\14\3\14\0\2\3\3\0\22\3\1\240\3\3\14\0"+
        "\2\3\3\0\13\3\1\241\12\3\14\0\2\3\3\0"+
        "\17\3\1\242\6\3\14\0\2\3\3\0\14\3\1\243"+
        "\11\3\13\0";
    
    /** 
     * The transition table of the DFA
     */
    final private static int yytrans [] = yy_unpack();
    
    
    /* error codes */
    final private static int YY_UNKNOWN_ERROR = 0;
    final private static int YY_NO_MATCH = 2;
    
    /* error messages for the codes above */
    final private static String YY_ERROR_MSG[] = {
        "Unkown internal scanner error",
        "Internal error: unknown state",
        "Error: could not match input",
        "Error: pushback value was too large"
    };
    
    /**
     * YY_ATTRIBUTE[aState] contains the attributes of state <code>aState</code>
     */
    private final static byte YY_ATTRIBUTE[] = {
        0,  9,  1,  1,  1,  9,  9,  1,  1,  1,  1,  1,  1,  1,  1,  1, 
        1,  1,  1,  1,  1,  1,  1,  1,  9,  9,  9,  9,  9,  1,  1,  1, 
        1,  1,  1,  0,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1, 
        1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1, 
        1,  9,  9,  9,  9,  0,  9,  0,  1,  1,  1,  1,  1,  1,  1,  1, 
        1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1, 
        1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1, 
        1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1, 
        1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1, 
        1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1, 
        1,  1,  1
    };
    
    /** the input device */
    private java.io.Reader yy_reader;
    
    /** the current state of the DFA */
    private int yy_state;
    
    /** the current lexical state */
    private int yy_lexical_state = YYINITIAL;
    
    /** this buffer contains the current text to be matched and is
      the source of the yytext() string */
    private char yy_buffer[] = new char[YY_BUFFERSIZE];
    
    /** the textposition at the last accepting state */
    private int yy_markedPos;
    
    /** the current text position in the buffer */
    private int yy_currentPos;
    
    /** startRead marks the beginning of the yytext() string in the buffer */
    private int yy_startRead;
    
    /** endRead marks the last character in the buffer, that has been read
      from input */
    private int yy_endRead;
    
    /** the number of characters up to the start of the matched text */
    private int yychar;
    
    /** yy_atEOF == true <=> the scanner is at the EOF */
    private boolean yy_atEOF;
    
    /** denotes if the user-EOF-code has already been executed */
    private boolean yy_eof_done;
    
    /* user code: */
    private Symbol symbol(int type) {
        return new Symbol(type, yychar, yychar + yylength());
    }
    
    private Symbol symbol(int type, Object value) {
        return new Symbol(type, yychar, yychar + yylength(), value);
    }
    
    
    /**
     * Creates a new scanner
     * There is also a java.io.InputStream version of this constructor.
     *
     * @param   in  the java.io.Reader to read input from.
     */
    public Lexer(java.io.Reader in) {
        this.yy_reader = in;
    }
    
    /**
     * Creates a new scanner.
     * There is also java.io.Reader version of this constructor.
     *
     * @param   in  the java.io.Inputstream to read input from.
     */
    public Lexer(java.io.InputStream in) {
        this(new java.io.InputStreamReader(in));
    }
    
    /** 
     * Unpacks the split, compressed DFA transition table.
     *
     * @return the unpacked transition table
     */
    private static int [] yy_unpack() {
        int [] trans = new int[4485];
        int offset = 0;
        offset = yy_unpack(yy_packed0, offset, trans);
        return trans;
    }
    
    /** 
     * Unpacks the compressed DFA transition table.
     *
     * @param packed   the packed transition table
     * @return         the index of the last entry
     */
    private static int yy_unpack(String packed, int offset, int [] trans) {
        int i = 0;       /* index in packed string  */
        int j = offset;  /* index in unpacked array */
        int l = packed.length();
        while (i < l) {
            int count = packed.charAt(i++);
            int value = packed.charAt(i++);
            value--;
            do trans[j++] = value; while (--count > 0);
        }
        return j;
    }
    
    /** 
     * Unpacks the compressed character translation table.
     *
     * @param packed   the packed character translation table
     * @return         the unpacked character translation table
     */
    private static char [] yy_unpack_cmap(String packed) {
        char [] map = new char[0x10000];
        int i = 0;  /* index in packed string  */
        int j = 0;  /* index in unpacked array */
        while (i < 154) {
            int  count = packed.charAt(i++);
            char value = packed.charAt(i++);
            do map[j++] = value; while (--count > 0);
        }
        return map;
    }
    
    
    /**
     * Refills the input buffer.
     *
     * @return      <code>false</code>, iff there was new input.
     * 
     * @exception   IOException  if any I/O-Error occurs
     */
    private boolean yy_refill() throws java.io.IOException {
        
        /* first: make room (if you can) */
        if (yy_startRead > 0) {
            System.arraycopy(yy_buffer, yy_startRead, 
                             yy_buffer, 0, 
                             yy_endRead-yy_startRead);
            
            /* translate stored positions */
            yy_endRead-= yy_startRead;
            yy_currentPos-= yy_startRead;
            yy_markedPos-= yy_startRead;
            yy_startRead = 0;
        }
        
        /* is the buffer big enough? */
        if (yy_currentPos >= yy_buffer.length) {
            /* if not: blow it up */
            char newBuffer[] = new char[yy_currentPos*2];
            System.arraycopy(yy_buffer, 0, newBuffer, 0, yy_buffer.length);
            yy_buffer = newBuffer;
        }
        
        /* finally: fill the buffer with new input */
        int numRead = yy_reader.read(yy_buffer, yy_endRead, 
                                     yy_buffer.length-yy_endRead);
        
        if (numRead < 0) {
            return true;
        }
        else {
            yy_endRead+= numRead;  
            return false;
        }
    }
    
    
    /**
     * Closes the input stream.
     */
    final public void yyclose() throws java.io.IOException {
        yy_atEOF = true;            /* indicate end of file */
        yy_endRead = yy_startRead;  /* invalidate buffer    */
        
        if (yy_reader != null)
            yy_reader.close();
    }
    
    
    /**
     * Closes the current stream, and resets the
     * scanner to read from a new input stream.
     *
     * All internal variables are reset, the old input stream 
     * <b>cannot</b> be reused (internal buffer is discarded and lost).
     * Lexical state is set to <tt>YY_INITIAL</tt>.
     *
     * @param reader   the new input stream 
     */
    final public void yyreset(java.io.Reader reader) throws java.io.IOException {
        yyclose();
        yy_reader = reader;
        yy_atEOF  = false;
        yy_endRead = yy_startRead = 0;
        yy_currentPos = yy_markedPos = 0;
        yychar = 0;
        yy_lexical_state = YYINITIAL;
    }
    
    
    /**
     * Returns the current lexical state.
     */
    final public int yystate() {
        return yy_lexical_state;
    }
    
    
    /**
     * Enters a new lexical state
     *
     * @param newState the new lexical state
     */
    final public void yybegin(int newState) {
        yy_lexical_state = newState;
    }
    
    
    /**
     * Returns the text matched by the current regular expression.
     */
    final public String yytext() {
        return new String( yy_buffer, yy_startRead, yy_markedPos-yy_startRead );
    }
    
    
    /**
     * Returns the character at position <tt>pos</tt> from the 
     * matched text. 
     * 
     * It is equivalent to yytext().charAt(pos), but faster
     *
     * @param pos the position of the character to fetch. 
     *            A value from 0 to yylength()-1.
     *
     * @return the character at position pos
     */
    final public char yycharat(int pos) {
        return yy_buffer[yy_startRead+pos];
    }
    
    
    /**
     * Returns the length of the matched text region.
     */
    final public int yylength() {
        return yy_markedPos-yy_startRead;
    }
    
    
    /**
     * Reports an error that occured while scanning.
     *
     * In a wellformed scanner (no or only correct usage of 
     * yypushback(int) and a match-all fallback rule) this method 
     * will only be called with things that "Can't Possibly Happen".
     * If this method is called, something is seriously wrong
     * (e.g. a JFlex bug producing a faulty scanner etc.).
     *
     * Usual syntax/scanner level error handling should be done
     * in error fallback rules.
     *
     * @param   errorCode  the code of the errormessage to display
     */
    private void yy_ScanError(int errorCode) {
        String message;
        try {
            message = YY_ERROR_MSG[errorCode];
        }
        catch (ArrayIndexOutOfBoundsException e) {
            message = YY_ERROR_MSG[YY_UNKNOWN_ERROR];
        }
        
        throw new Error(message);
    }
    
    /**
     * Contains user EOF-code, which will be executed exactly once,
     * when the end of file is reached
     */
    private void yy_do_eof() throws java.io.IOException {
        if (!yy_eof_done) {
            yy_eof_done = true;
            yyclose();
        }
    }
    
    
    /**
     * Resumes scanning until the next regular expression is matched,
     * the end of input is encountered or an I/O-Error occurs.
     *
     * @return      the next token
     * @exception   IOException  if any I/O-Error occurs
     */
    public java_cup.runtime.Symbol next_token() throws java.io.IOException {
        int yy_input;
        int yy_action;
        
        // cached fields:
        int yy_currentPos_l;
        int yy_markedPos_l;
        int yy_endRead_l = yy_endRead;
        char [] yy_buffer_l = yy_buffer;
        char [] yycmap_l = yycmap;
        
        int [] yytrans_l = yytrans;
        int [] yy_rowMap_l = yy_rowMap;
        byte [] yy_attr_l = YY_ATTRIBUTE;
        
        while (true) {
            yy_markedPos_l = yy_markedPos;
            
            yychar+= yy_markedPos_l-yy_startRead;
            
            yy_action = -1;
            
            yy_currentPos_l = yy_currentPos = 
                yy_startRead = yy_markedPos_l;
            
            yy_state = yy_lexical_state;
            
            
            yy_forAction: {
                while (true) {
                    
                    if (yy_currentPos_l < yy_endRead_l)
                        yy_input = yy_buffer_l[yy_currentPos_l++];
                    else if (yy_atEOF) {
                        yy_input = YYEOF;
                        break yy_forAction;
                    }
                    else {
                        // store back cached positions
                        yy_currentPos  = yy_currentPos_l;
                        yy_markedPos   = yy_markedPos_l;
                        boolean eof = yy_refill();
                        // get translated positions and possibly new buffer
                        yy_currentPos_l  = yy_currentPos;
                        yy_markedPos_l   = yy_markedPos;
                        yy_buffer_l      = yy_buffer;
                        yy_endRead_l     = yy_endRead;
                        if (eof) {
                            yy_input = YYEOF;
                            break yy_forAction;
                        }
                        else {
                            yy_input = yy_buffer_l[yy_currentPos_l++];
                        }
                    }
                    int yy_next = yytrans_l[ yy_rowMap_l[yy_state] + yycmap_l[yy_input] ];
                    if (yy_next == -1) break yy_forAction;
                    yy_state = yy_next;
                    
                    int yy_attributes = yy_attr_l[yy_state];
                    if ( (yy_attributes & 1) == 1 ) {
                        yy_action = yy_state; 
                        yy_markedPos_l = yy_currentPos_l; 
                        if ( (yy_attributes & 8) == 8 ) break yy_forAction;
                    }
                    
                }
            }
            
            // store back cached position
            yy_markedPos = yy_markedPos_l;
            
            switch (yy_action) {
                
                case 3: 
                case 36: 
                {  return symbol(sym.INT_VAL, new Integer(yytext()));  }
                case 164: break;
                case 160: 
                {  return symbol(sym.VARCHAR);  }
                case 165: break;
                case 159: 
                {  return symbol(sym.PRIMARY);  }
                case 166: break;
                case 157: 
                {  return symbol(sym.INTEGER);  }
                case 167: break;
                case 154: 
                {  return symbol(sym.VALUES);  }
                case 168: break;
                case 153: 
                {  return symbol(sym.UPDATE);  }
                case 169: break;
                case 150: 
                {  return symbol(sym.COMMIT);  }
                case 170: break;
                case 149: 
                {  return symbol(sym.CREATE);  }
                case 171: break;
                case 147: 
                {  return symbol(sym.INSERT);  }
                case 172: break;
                case 146: 
                {  return symbol(sym.SELECT);  }
                case 173: break;
                case 144: 
                {  return symbol(sym.DELETE);  }
                case 174: break;
                case 24: 
                {  return symbol(sym.LPAREN);  }
                case 175: break;
                case 25: 
                {  return symbol(sym.RPAREN);  }
                case 176: break;
                case 86: 
                {  return symbol(sym.INTEGER);  }
                case 177: break;
                case 162: 
                {  return symbol(sym.ROLLBACK);  }
                case 178: break;
                case 161: 
                {  return symbol(sym.DISTINCT);  }
                case 179: break;
                case 28: 
                {  return symbol(sym.SEMICOLON);  }
                case 180: break;
                case 6: 
                {  /* skip it */  }
                case 181: break;
                case 57: 
                {  return symbol(sym.OR);  }
                case 182: break;
                case 49: 
                {  return symbol(sym.IS);  }
                case 183: break;
                case 39: 
                {  return symbol(sym.AS);  }
                case 184: break;
                case 32: 
                {  return symbol(sym.NOT);  }
                case 185: break;
                case 31: 
                {  return symbol(sym.LT);  }
                case 186: break;
                case 30: 
                {  return symbol(sym.GT);  }
                case 187: break;
                case 29: 
                {  return symbol(sym.EQ);  }
                case 188: break;
                case 5: 
                {  return symbol(sym.DOT);  }
                case 189: break;
                case 65: 
                {  return symbol(sym.EQ);  }
                case 190: break;
                case 73: 
                {  return symbol(sym.ALL);  }
                case 191: break;
                case 74: 
                {  return symbol(sym.AND);  }
                case 192: break;
                case 77: 
                {  return symbol(sym.NOT);  }
                case 193: break;
                case 83: 
                {  return symbol(sym.SET);  }
                case 194: break;
                case 93: 
                {  return symbol(sym.KEY);  }
                case 195: break;
                case 2: 
                case 7: 
                case 8: 
                case 9: 
                case 10: 
                case 11: 
                case 12: 
                case 13: 
                case 14: 
                case 15: 
                case 16: 
                case 17: 
                case 18: 
                case 19: 
                case 20: 
                case 21: 
                case 22: 
                case 23: 
                case 37: 
                case 38: 
                case 40: 
                case 41: 
                case 42: 
                case 43: 
                case 44: 
                case 45: 
                case 46: 
                case 47: 
                case 48: 
                case 50: 
                case 51: 
                case 52: 
                case 53: 
                case 54: 
                case 55: 
                case 56: 
                case 58: 
                case 59: 
                case 60: 
                case 61: 
                case 62: 
                case 63: 
                case 64: 
                case 75: 
                case 76: 
                case 78: 
                case 79: 
                case 80: 
                case 81: 
                case 82: 
                case 84: 
                case 85: 
                case 87: 
                case 88: 
                case 89: 
                case 90: 
                case 91: 
                case 92: 
                case 94: 
                case 95: 
                case 96: 
                case 97: 
                case 98: 
                case 99: 
                case 100: 
                case 101: 
                case 103: 
                case 105: 
                case 106: 
                case 108: 
                case 109: 
                case 110: 
                case 111: 
                case 113: 
                case 115: 
                case 116: 
                case 118: 
                case 119: 
                case 120: 
                case 122: 
                case 123: 
                case 124: 
                case 125: 
                case 128: 
                case 129: 
                case 130: 
                case 132: 
                case 133: 
                case 135: 
                case 136: 
                case 137: 
                case 139: 
                case 140: 
                case 141: 
                case 142: 
                case 145: 
                case 148: 
                case 151: 
                case 152: 
                case 155: 
                case 156: 
                case 158: 
                {  return symbol(sym.ID, yytext());  }
                case 196: break;
                case 143: 
                {  return symbol(sym.WHERE);  }
                case 197: break;
                case 138: 
                {  return symbol(sym.TABLE);  }
                case 198: break;
                case 134: 
                {  return symbol(sym.CLIKE);  }
                case 199: break;
                case 131: 
                {  return symbol(sym.BEGIN);  }
                case 200: break;
                case 127: 
                {  return symbol(sym.LIMIT);  }
                case 201: break;
                case 126: 
                {  return symbol(sym.WORK);  }
                case 202: break;
                case 121: 
                {  return symbol(sym.FROM);  }
                case 203: break;
                case 27: 
                {  return symbol(sym.STAR);  }
                case 204: break;
                case 26: 
                {  return symbol(sym.COMMA);  }
                case 205: break;
                case 66: 
                {  return symbol(sym.GTEQ);  }
                case 206: break;
                case 67: 
                {  return symbol(sym.LTEQ);  }
                case 207: break;
                case 68: 
                {  return symbol(sym.NOTEQ);  }
                case 208: break;
                case 102: 
                {  return symbol(sym.LIKE);  }
                case 209: break;
                case 104: 
                {  return symbol(sym.NULL);  }
                case 210: break;
                case 107: 
                {  return symbol(sym.DROP);  }
                case 211: break;
                case 112: 
                {  return symbol(sym.INTO);  }
                case 212: break;
                case 114: 
                {  return symbol(sym.CHAR);  }
                case 213: break;
                case 117: 
                {  return symbol(sym.REAL);  }
                case 214: break;
                case 1: 
                case 4: 
                case 33: 
                case 34: 
                {  
                    throw new RuntimeException("Illegal character \"" + 
                                               yytext() + "\""); 
                }
                case 215: break;
                case 70: 
                { 
                    String str = yytext().substring(1, yylength() - 1);
                    return symbol(sym.STRING, str);
                }
                case 216: break;
                case 72: 
                {  return symbol(sym.REAL_VAL, new Double(yytext()));  }
                case 217: break;
                default: 
                    if (yy_input == YYEOF && yy_startRead == yy_currentPos) {
                    yy_atEOF = true;
                    yy_do_eof();
                    { return new java_cup.runtime.Symbol(sym.EOF); }
                } 
                    else {
                        yy_ScanError(YY_NO_MATCH);
                    }
            }
        }
    }
    
    /**
     * Converts an int token code into the name of the
     * token by reflection on the cup symbol class/interface sym
     *
     * This code was contributed by Karl Meissner <meissnersd@yahoo.com>
     * <a href="http://meissner.v0.net/msd.htm">meissner.v0.net/msd.htm</a>
     */
    private String getTokenName(int token) {
        try {
            java.lang.reflect.Field [] class_fields = sym.class.getFields();
            for (int i = 0; i < class_fields.length; i++) {
                if (class_fields[i].getInt(null) == token) {
                    return class_fields[i].getName();
                }
            }
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
        
        return "UNKNOWN TOKEN";
    }
    
    /**
     * Same as next_token but also prints the token to standard out
     * for debugging.
     *
     * This code was contributed by Karl Meissner <meissnersd@yahoo.com>
     * <a href="http://meissner.v0.net/msd.htm">meissner.v0.net/msd.htm</a>
     */
    public java_cup.runtime.Symbol debug_next_token() throws java.io.IOException {
        java_cup.runtime.Symbol s = next_token();
        System.out.println( " --"+ yytext() + "--" + getTokenName(s.sym) + "--");
        return s;
    }
    
    /**
     * Runs the scanner on input files.
     *
     * This main method is the debugging routine for the scanner.
     * It prints debugging information about each returned token to
     * System.out until the end of file is reached, or an error occured.
     *
     * @param argv   the command line, contains the filenames to run
     *               the scanner on.
     */
    public static void main(String argv[]) {
        if (argv.length == 0) {
            System.out.println("Usage : java Lexer <inputfile>");
        }
        else {
            for (int i = 0; i < argv.length; i++) {
                Lexer scanner = null;
                try {
                    scanner = new Lexer( new java.io.FileReader(argv[i]) );
                    while ( !scanner.yy_atEOF ) scanner.debug_next_token();
                }
                catch (java.io.FileNotFoundException e) {
                    System.out.println("File not found : \""+argv[i]+"\"");
                }
                catch (java.io.IOException e) {
                    System.out.println("IO error scanning file \""+argv[i]+"\"");
                    System.out.println(e);
                }
                catch (Exception e) {
                    System.out.println("Unexpected exception:");
                    e.printStackTrace();
                }
            }
        }
    }    
}
