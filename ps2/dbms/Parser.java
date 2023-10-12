import java.util.*;
import java_cup.runtime.*;

public class Parser extends java_cup.runtime.lr_parser {
    
    /** Default constructor. */
    public Parser() {super();}
    
    /** Constructor which sets the default scanner. */
    public Parser(java_cup.runtime.Scanner s) {super(s);}
    
    /** Production table. */
    protected static final short _production_table[][] = 
        unpackFromStrings(new String[] {
        "\000\130\000\002\002\004\000\002\002\004\000\002\002" +
            "\004\000\002\002\004\000\002\002\004\000\002\002\004" +
            "\000\002\002\004\000\002\002\004\000\002\002\004\000" +
            "\002\002\004\000\002\010\005\000\002\011\004\000\002" +
            "\012\004\000\002\013\004\000\002\044\002\000\002\044" +
            "\003\000\002\007\010\000\002\032\003\000\002\032\005" +
            "\000\002\017\005\000\002\017\010\000\002\017\010\000" +
            "\002\017\005\000\002\020\002\000\002\020\004\000\002" +
            "\020\004\000\002\020\006\000\002\020\006\000\002\003" +
            "\011\000\002\040\002\000\002\040\003\000\002\040\003" +
            "\000\002\036\003\000\002\036\005\000\002\042\003\000" +
            "\002\042\003\000\002\042\003\000\002\016\003\000\002" +
            "\016\005\000\002\041\003\000\002\041\003\000\002\041" +
            "\003\000\002\041\003\000\002\031\003\000\002\031\005" +
            "\000\002\015\004\000\002\014\003\000\002\043\002\000" +
            "\002\043\003\000\002\043\004\000\002\021\002\000\002" +
            "\021\004\000\002\022\003\000\002\022\005\000\002\023" +
            "\003\000\002\023\005\000\002\024\003\000\002\024\004" +
            "\000\002\025\003\000\002\025\005\000\002\026\005\000" +
            "\002\026\005\000\002\026\005\000\002\026\005\000\002" +
            "\026\005\000\002\026\005\000\002\026\005\000\002\026" +
            "\005\000\002\026\005\000\002\026\006\000\002\027\003" +
            "\000\002\027\003\000\002\027\003\000\002\027\003\000" +
            "\002\030\002\000\002\030\004\000\002\030\006\000\002" +
            "\004\012\000\002\033\002\000\002\033\005\000\002\034" +
            "\003\000\002\034\005\000\002\037\003\000\002\037\005" +
            "\000\002\006\006\000\002\005\007\000\002\035\005\000" +
            "\002\035\007" });
    
    /** Access to production table. */
    public short[][] production_table() {return _production_table;}
    
    /** Parse-action table. */
    protected static final short[][] _action_table = 
        unpackFromStrings(new String[] {
        "\000\254\000\024\007\014\012\013\013\015\014\022\016" +
            "\020\020\016\034\011\035\005\040\023\001\002\000\004" +
            "\052\256\001\002\000\022\004\225\015\224\030\uffe4\051" +
            "\uffe4\061\uffe4\062\uffe4\063\uffe4\064\uffe4\001\002\000\004" +
            "\052\223\001\002\000\004\052\222\001\002\000\004\052" +
            "\221\001\002\000\006\044\216\052\ufff3\001\002\000\004" +
            "\002\217\001\002\000\006\044\216\052\ufff3\001\002\000" +
            "\004\044\214\001\002\000\004\037\155\001\002\000\004" +
            "\022\135\001\002\000\004\052\134\001\002\000\004\037" +
            "\132\001\002\000\004\052\131\001\002\000\004\017\126" +
            "\001\002\000\004\061\032\001\002\000\004\052\031\001" +
            "\002\000\004\052\030\001\002\000\004\052\027\001\002" +
            "\000\004\002\uffff\001\002\000\004\002\ufffd\001\002\000" +
            "\004\002\ufffa\001\002\000\024\006\uffd3\026\uffd3\036\uffd3" +
            "\041\uffd3\043\uffd3\045\uffd3\047\uffd3\052\uffd3\061\uffd3\001" +
            "\002\000\004\036\034\001\002\000\004\061\035\001\002" +
            "\000\054\005\uffdc\010\uffdc\011\uffdc\017\uffdc\021\uffdc\023" +
            "\uffdc\025\uffdc\026\uffdc\031\uffdc\033\uffdc\042\uffdc\046\uffdc" +
            "\047\uffdc\050\uffd3\052\uffdc\053\uffdc\054\uffdc\055\uffdc\056" +
            "\uffdc\057\uffdc\060\uffdc\001\002\000\010\043\053\047\051" +
            "\052\uffcf\001\002\000\004\053\043\001\002\000\004\050" +
            "\041\001\002\000\004\061\042\001\002\000\052\005\uffdb" +
            "\010\uffdb\011\uffdb\017\uffdb\021\uffdb\023\uffdb\025\uffdb\026" +
            "\uffdb\031\uffdb\033\uffdb\042\uffdb\046\uffdb\047\uffdb\052\uffdb" +
            "\053\uffdb\054\uffdb\055\uffdb\056\uffdb\057\uffdb\060\uffdb\001" +
            "\002\000\012\030\050\062\046\063\047\064\045\001\002" +
            "\000\010\043\uffab\047\uffab\052\uffab\001\002\000\014\017" +
            "\uffd9\043\uffd9\046\uffd9\047\uffd9\052\uffd9\001\002\000\014" +
            "\017\uffd8\043\uffd8\046\uffd8\047\uffd8\052\uffd8\001\002\000" +
            "\014\017\uffda\043\uffda\046\uffda\047\uffda\052\uffda\001\002" +
            "\000\014\017\uffd7\043\uffd7\046\uffd7\047\uffd7\052\uffd7\001" +
            "\002\000\004\061\035\001\002\000\004\052\uffac\001\002" +
            "\000\016\027\064\045\066\061\035\062\060\063\056\064" +
            "\065\001\002\000\014\005\uffc9\026\uffc9\031\uffc9\046\uffc9" +
            "\052\uffc9\001\002\000\010\026\uffce\031\072\052\uffce\001" +
            "\002\000\036\005\uffba\011\uffba\023\uffba\025\uffba\026\uffba" +
            "\031\uffba\046\uffba\052\uffba\053\uffba\054\uffba\055\uffba\056" +
            "\uffba\057\uffba\060\uffba\001\002\000\036\005\uffbb\011\uffbb" +
            "\023\uffbb\025\uffbb\026\uffbb\031\uffbb\046\uffbb\052\uffbb\053" +
            "\uffbb\054\uffbb\055\uffbb\056\uffbb\057\uffbb\060\uffbb\001\002" +
            "\000\036\005\uffb8\011\uffb8\023\uffb8\025\uffb8\026\uffb8\031" +
            "\uffb8\046\uffb8\052\uffb8\053\uffb8\054\uffb8\055\uffb8\056\uffb8" +
            "\057\uffb8\060\uffb8\001\002\000\014\005\uffcb\026\uffcb\031" +
            "\uffcb\046\uffcb\052\uffcb\001\002\000\014\005\074\026\uffcd" +
            "\031\uffcd\046\uffcd\052\uffcd\001\002\000\024\011\077\023" +
            "\107\025\106\053\102\054\103\055\105\056\104\057\100" +
            "\060\101\001\002\000\014\045\066\061\035\062\060\063" +
            "\056\064\065\001\002\000\036\005\uffb9\011\uffb9\023\uffb9" +
            "\025\uffb9\026\uffb9\031\uffb9\046\uffb9\052\uffb9\053\uffb9\054" +
            "\uffb9\055\uffb9\056\uffb9\057\uffb9\060\uffb9\001\002\000\016" +
            "\027\064\045\066\061\035\062\060\063\056\064\065\001" +
            "\002\000\014\005\uffc7\026\uffc7\031\uffc7\046\uffc7\052\uffc7" +
            "\001\002\000\006\031\072\046\071\001\002\000\014\005" +
            "\uffc6\026\uffc6\031\uffc6\046\uffc6\052\uffc6\001\002\000\016" +
            "\027\064\045\066\061\035\062\060\063\056\064\065\001" +
            "\002\000\014\005\074\026\uffcc\031\uffcc\046\uffcc\052\uffcc" +
            "\001\002\000\016\027\064\045\066\061\035\062\060\063" +
            "\056\064\065\001\002\000\014\005\uffca\026\uffca\031\uffca" +
            "\046\uffca\052\uffca\001\002\000\014\005\uffc8\026\uffc8\031" +
            "\uffc8\046\uffc8\052\uffc8\001\002\000\004\062\122\001\002" +
            "\000\012\061\035\062\060\063\056\064\065\001\002\000" +
            "\012\061\035\062\060\063\056\064\065\001\002\000\012" +
            "\061\035\062\060\063\056\064\065\001\002\000\012\061" +
            "\035\062\060\063\056\064\065\001\002\000\012\061\035" +
            "\062\060\063\056\064\065\001\002\000\012\061\035\062" +
            "\060\063\056\064\065\001\002\000\004\062\113\001\002" +
            "\000\006\027\110\030\111\001\002\000\004\030\112\001" +
            "\002\000\014\005\uffbd\026\uffbd\031\uffbd\046\uffbd\052\uffbd" +
            "\001\002\000\014\005\uffbc\026\uffbc\031\uffbc\046\uffbc\052" +
            "\uffbc\001\002\000\014\005\uffbf\026\uffbf\031\uffbf\046\uffbf" +
            "\052\uffbf\001\002\000\014\005\uffc3\026\uffc3\031\uffc3\046" +
            "\uffc3\052\uffc3\001\002\000\014\005\uffc1\026\uffc1\031\uffc1" +
            "\046\uffc1\052\uffc1\001\002\000\014\005\uffc2\026\uffc2\031" +
            "\uffc2\046\uffc2\052\uffc2\001\002\000\014\005\uffc5\026\uffc5" +
            "\031\uffc5\046\uffc5\052\uffc5\001\002\000\014\005\uffc4\026" +
            "\uffc4\031\uffc4\046\uffc4\052\uffc4\001\002\000\014\005\uffc0" +
            "\026\uffc0\031\uffc0\046\uffc0\052\uffc0\001\002\000\014\005" +
            "\uffbe\026\uffbe\031\uffbe\046\uffbe\052\uffbe\001\002\000\004" +
            "\053\124\001\002\000\012\030\050\062\046\063\047\064" +
            "\045\001\002\000\010\043\uffaa\047\uffaa\052\uffaa\001\002" +
            "\000\004\061\032\001\002\000\006\043\053\052\uffcf\001" +
            "\002\000\004\052\uffad\001\002\000\004\002\ufffc\001\002" +
            "\000\004\061\032\001\002\000\004\052\ufff7\001\002\000" +
            "\004\002\001\001\002\000\004\061\032\001\002\000\006" +
            "\041\uffb3\045\140\001\002\000\004\041\146\001\002\000" +
            "\004\061\035\001\002\000\006\046\144\047\143\001\002" +
            "\000\006\046\uffb1\047\uffb1\001\002\000\004\061\035\001" +
            "\002\000\004\041\uffb2\001\002\000\006\046\uffb0\047\uffb0" +
            "\001\002\000\004\045\147\001\002\000\012\030\050\062" +
            "\046\063\047\064\045\001\002\000\006\046\153\047\152" +
            "\001\002\000\006\046\uffaf\047\uffaf\001\002\000\012\030" +
            "\050\062\046\063\047\064\045\001\002\000\004\052\uffb4" +
            "\001\002\000\006\046\uffae\047\uffae\001\002\000\004\061" +
            "\032\001\002\000\004\045\157\001\002\000\004\061\035" +
            "\001\002\000\006\046\ufff0\047\ufff0\001\002\000\012\010" +
            "\170\021\166\033\167\042\171\001\002\000\006\046\164" +
            "\047\163\001\002\000\004\061\035\001\002\000\004\052" +
            "\ufff1\001\002\000\006\046\uffef\047\uffef\001\002\000\012" +
            "\027\176\032\175\046\uffea\047\uffea\001\002\000\012\027" +
            "\176\032\175\046\uffea\047\uffea\001\002\000\004\045\206" +
            "\001\002\000\004\045\172\001\002\000\004\063\173\001" +
            "\002\000\004\046\174\001\002\000\012\027\176\032\175" +
            "\046\uffea\047\uffea\001\002\000\004\024\203\001\002\000" +
            "\004\030\200\001\002\000\006\046\uffec\047\uffec\001\002" +
            "\000\010\032\201\046\uffe9\047\uffe9\001\002\000\004\024" +
            "\202\001\002\000\006\046\uffe7\047\uffe7\001\002\000\010" +
            "\027\204\046\uffe8\047\uffe8\001\002\000\004\030\205\001" +
            "\002\000\006\046\uffe6\047\uffe6\001\002\000\004\063\207" +
            "\001\002\000\004\046\210\001\002\000\012\027\176\032" +
            "\175\046\uffea\047\uffea\001\002\000\006\046\uffed\047\uffed" +
            "\001\002\000\006\046\uffeb\047\uffeb\001\002\000\006\046" +
            "\uffee\047\uffee\001\002\000\004\052\ufff6\001\002\000\004" +
            "\052\ufff5\001\002\000\004\052\ufff2\001\002\000\004\002" +
            "\000\001\002\000\004\052\ufff4\001\002\000\004\002\ufff9" +
            "\001\002\000\004\002\ufffe\001\002\000\004\002\ufffb\001" +
            "\002\000\016\030\uffe2\051\uffe2\061\uffe2\062\uffe2\063\uffe2" +
            "\064\uffe2\001\002\000\016\030\uffe3\051\uffe3\061\uffe3\062" +
            "\uffe3\063\uffe3\064\uffe3\001\002\000\016\030\050\051\232" +
            "\061\035\062\046\063\047\064\045\001\002\000\006\017" +
            "\uffe1\047\uffe1\001\002\000\006\017\uffde\047\uffde\001\002" +
            "\000\006\017\uffdf\047\uffdf\001\002\000\006\017\uffdd\047" +
            "\uffdd\001\002\000\006\017\235\047\234\001\002\000\016" +
            "\030\050\051\232\061\035\062\046\063\047\064\045\001" +
            "\002\000\004\061\032\001\002\000\012\026\uffd6\043\uffd6" +
            "\047\uffd6\052\uffd6\001\002\000\012\026\uffcf\043\053\047" +
            "\245\052\uffcf\001\002\000\016\006\242\026\uffd2\043\uffd2" +
            "\047\uffd2\052\uffd2\061\241\001\002\000\012\026\uffd1\043" +
            "\uffd1\047\uffd1\052\uffd1\001\002\000\004\061\244\001\002" +
            "\000\012\026\uffd4\043\uffd4\047\uffd4\052\uffd4\001\002\000" +
            "\012\026\uffd0\043\uffd0\047\uffd0\052\uffd0\001\002\000\004" +
            "\061\032\001\002\000\006\026\247\052\uffb7\001\002\000" +
            "\004\063\251\001\002\000\004\052\uffe5\001\002\000\006" +
            "\047\252\052\uffb6\001\002\000\004\063\253\001\002\000" +
            "\004\052\uffb5\001\002\000\012\026\uffd5\043\uffd5\047\uffd5" +
            "\052\uffd5\001\002\000\006\017\uffe0\047\uffe0\001\002\000" +
            "\004\002\ufff8\001\002" });
    
    /** Access to parse-action table. */
    public short[][] action_table() {return _action_table;}
    
    /** <code>reduce_goto</code> table. */
    protected static final short[][] _reduce_table = 
        unpackFromStrings(new String[] {
        "\000\254\000\026\002\011\003\016\004\025\005\006\006" +
            "\024\007\020\010\005\011\023\012\007\013\003\001\001" +
            "\000\002\001\001\000\004\040\225\001\001\000\002\001" +
            "\001\000\002\001\001\000\002\001\001\000\004\044\217" +
            "\001\001\000\002\001\001\000\004\044\214\001\001\000" +
            "\002\001\001\000\002\001\001\000\002\001\001\000\002" +
            "\001\001\000\002\001\001\000\002\001\001\000\002\001" +
            "\001\000\004\014\032\001\001\000\002\001\001\000\002" +
            "\001\001\000\002\001\001\000\002\001\001\000\002\001" +
            "\001\000\002\001\001\000\002\001\001\000\002\001\001" +
            "\000\010\014\037\016\036\035\035\001\001\000\002\001" +
            "\001\000\004\021\051\001\001\000\002\001\001\000\002" +
            "\001\001\000\002\001\001\000\002\001\001\000\004\041" +
            "\043\001\001\000\002\001\001\000\002\001\001\000\002" +
            "\001\001\000\002\001\001\000\002\001\001\000\006\014" +
            "\037\016\122\001\001\000\002\001\001\000\022\014\037" +
            "\016\056\022\054\023\061\024\060\025\053\026\066\027" +
            "\062\001\001\000\002\001\001\000\002\001\001\000\002" +
            "\001\001\000\002\001\001\000\002\001\001\000\002\001" +
            "\001\000\002\001\001\000\002\001\001\000\014\014\037" +
            "\016\056\025\075\026\066\027\062\001\001\000\002\001" +
            "\001\000\022\014\037\016\056\022\067\023\061\024\060" +
            "\025\053\026\066\027\062\001\001\000\002\001\001\000" +
            "\002\001\001\000\002\001\001\000\020\014\037\016\056" +
            "\023\072\024\060\025\053\026\066\027\062\001\001\000" +
            "\002\001\001\000\016\014\037\016\056\024\074\025\053" +
            "\026\066\027\062\001\001\000\002\001\001\000\002\001" +
            "\001\000\002\001\001\000\010\014\037\016\056\027\120" +
            "\001\001\000\010\014\037\016\056\027\117\001\001\000" +
            "\010\014\037\016\056\027\116\001\001\000\010\014\037" +
            "\016\056\027\115\001\001\000\010\014\037\016\056\027" +
            "\114\001\001\000\010\014\037\016\056\027\113\001\001" +
            "\000\002\001\001\000\002\001\001\000\002\001\001\000" +
            "\002\001\001\000\002\001\001\000\002\001\001\000\002" +
            "\001\001\000\002\001\001\000\002\001\001\000\002\001" +
            "\001\000\002\001\001\000\002\001\001\000\002\001\001" +
            "\000\002\001\001\000\004\041\124\001\001\000\002\001" +
            "\001\000\004\014\126\001\001\000\004\021\127\001\001" +
            "\000\002\001\001\000\002\001\001\000\004\014\132\001" +
            "\001\000\002\001\001\000\002\001\001\000\004\014\135" +
            "\001\001\000\004\033\136\001\001\000\002\001\001\000" +
            "\010\014\037\016\141\034\140\001\001\000\002\001\001" +
            "\000\002\001\001\000\006\014\037\016\144\001\001\000" +
            "\002\001\001\000\002\001\001\000\002\001\001\000\006" +
            "\037\147\041\150\001\001\000\002\001\001\000\002\001" +
            "\001\000\004\041\153\001\001\000\002\001\001\000\002" +
            "\001\001\000\004\014\155\001\001\000\002\001\001\000" +
            "\012\014\037\016\160\017\157\032\161\001\001\000\002" +
            "\001\001\000\002\001\001\000\002\001\001\000\010\014" +
            "\037\016\160\017\164\001\001\000\002\001\001\000\002" +
            "\001\001\000\004\020\212\001\001\000\004\020\211\001" +
            "\001\000\002\001\001\000\002\001\001\000\002\001\001" +
            "\000\002\001\001\000\004\020\176\001\001\000\002\001" +
            "\001\000\002\001\001\000\002\001\001\000\002\001\001" +
            "\000\002\001\001\000\002\001\001\000\002\001\001\000" +
            "\002\001\001\000\002\001\001\000\002\001\001\000\002" +
            "\001\001\000\004\020\210\001\001\000\002\001\001\000" +
            "\002\001\001\000\002\001\001\000\002\001\001\000\002" +
            "\001\001\000\002\001\001\000\002\001\001\000\002\001" +
            "\001\000\002\001\001\000\002\001\001\000\002\001\001" +
            "\000\002\001\001\000\002\001\001\000\014\014\037\016" +
            "\230\036\232\041\227\042\226\001\001\000\002\001\001" +
            "\000\002\001\001\000\002\001\001\000\002\001\001\000" +
            "\002\001\001\000\012\014\037\016\230\041\227\042\254" +
            "\001\001\000\010\014\237\015\235\031\236\001\001\000" +
            "\002\001\001\000\004\021\245\001\001\000\004\043\242" +
            "\001\001\000\002\001\001\000\002\001\001\000\002\001" +
            "\001\000\002\001\001\000\006\014\237\015\253\001\001" +
            "\000\004\030\247\001\001\000\002\001\001\000\002\001" +
            "\001\000\002\001\001\000\002\001\001\000\002\001\001" +
            "\000\002\001\001\000\002\001\001\000\002\001\001" });
    
    /** Access to <code>reduce_goto</code> table. */
    public short[][] reduce_table() {return _reduce_table;}
    
    /** Instance of action encapsulation class. */
    protected CUP$Parser$actions action_obj;
    
    /** Action encapsulation object initializer. */
    protected void init_actions()
    {
        action_obj = new CUP$Parser$actions(this);
    }
    
    /** Invoke a user supplied parse action. */
    public java_cup.runtime.Symbol do_action(
                                             int                        act_num,
                                             java_cup.runtime.lr_parser parser,
                                             java.util.Stack            stack,
                                             int                        top)
        throws java.lang.Exception
    {
        /* call code in generated class */
        return action_obj.CUP$Parser$do_action(act_num, parser, stack, top);
    }
    
    /** Indicates start state. */
    public int start_state() {return 0;}
    /** Indicates start production. */
    public int start_production() {return 1;}
    
    /** <code>EOF</code> Symbol index. */
    public int EOF_sym() {return 0;}
    
    /** <code>error</code> Symbol index. */
    public int error_sym() {return 1;}
    
    
    
    public void syntax_error(Symbol cur_token) {
        System.err.println("Syntax error beginning at character " +
                           cur_token.left + ": \"" + cur_token.value + "\"");
    }
    public void unrecovered_syntax_error(Symbol cur_token) {
        throw new InvalidSyntaxException();
    }
    
}

/** Cup generated class to encapsulate user supplied action code.*/
class CUP$Parser$actions {
    private final Parser parser;
    
    /** Constructor */
    CUP$Parser$actions(Parser parser) {
        this.parser = parser;
    }
    
    /** Method with the actual generated action code. */
    public final java_cup.runtime.Symbol CUP$Parser$do_action(
                                                              int                        CUP$Parser$act_num,
                                                              java_cup.runtime.lr_parser CUP$Parser$parser,
                                                              java.util.Stack            CUP$Parser$stack,
                                                              int                        CUP$Parser$top)
        throws java.lang.Exception
    {
        /* Symbol object for return from actions */
        java_cup.runtime.Symbol CUP$Parser$result;
        
        /* select the action based on the action number */
        switch (CUP$Parser$act_num)
        {
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 87: // update_list ::= update_list COMMA column EQ column_val 
            {
                ArrayList<Column> RESULT = null;
                int ulleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-4)).left;
                int ulright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-4)).right;
                ArrayList<Column> ul = (ArrayList<Column>)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-4)).value;
                int cleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left;
                int cright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).right;
                Column c = (Column)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-2)).value;
                int vleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int vright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                Object v = (Object)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = ul; c.setUpdateVal(v); RESULT.add(c); 
                CUP$Parser$result = new java_cup.runtime.Symbol(27/*update_list*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-4)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 86: // update_list ::= column EQ column_val 
            {
                ArrayList<Column> RESULT = null;
                int cleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left;
                int cright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).right;
                Column c = (Column)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-2)).value;
                int vleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int vright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                Object v = (Object)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new ArrayList<Column>(); c.setUpdateVal(v); RESULT.add(c); 
                CUP$Parser$result = new java_cup.runtime.Symbol(27/*update_list*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 85: // update_stmt ::= UPDATE table SET update_list where_clause 
            {
                UpdateStatement RESULT = null;
                int tleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-3)).left;
                int tright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-3)).right;
                Table t = (Table)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-3)).value;
                int ulleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left;
                int ulright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).right;
                ArrayList<Column> ul = (ArrayList<Column>)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-1)).value;
                int wleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int wright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                ConditionalExpression w = (ConditionalExpression)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new UpdateStatement(t, ul, w); 
                CUP$Parser$result = new java_cup.runtime.Symbol(3/*update_stmt*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-4)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 84: // delete_stmt ::= DELETE FROM table where_clause 
            {
                DeleteStatement RESULT = null;
                int tleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left;
                int tright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).right;
                Table t = (Table)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-1)).value;
                int wleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int wright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                ConditionalExpression w = (ConditionalExpression)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new DeleteStatement(t, w); 
                CUP$Parser$result = new java_cup.runtime.Symbol(4/*delete_stmt*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-3)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 83: // insert_val_list ::= insert_val_list COMMA column_val 
            {
                ArrayList<Object> RESULT = null;
                int vlleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left;
                int vlright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).right;
                ArrayList<Object> vl = (ArrayList<Object>)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-2)).value;
                int vleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int vright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                Object v = (Object)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = vl; RESULT.add(v); 
                CUP$Parser$result = new java_cup.runtime.Symbol(29/*insert_val_list*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 82: // insert_val_list ::= column_val 
            {
                ArrayList<Object> RESULT = null;
                int vleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int vright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                Object v = (Object)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new ArrayList<Object>(); RESULT.add(v); 
                CUP$Parser$result = new java_cup.runtime.Symbol(29/*insert_val_list*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 81: // insert_col_list ::= insert_col_list COMMA column 
            {
                ArrayList<Column> RESULT = null;
                int clleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left;
                int clright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).right;
                ArrayList<Column> cl = (ArrayList<Column>)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-2)).value;
                int cleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int cright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                Column c = (Column)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = cl; RESULT.add(c); 
                CUP$Parser$result = new java_cup.runtime.Symbol(26/*insert_col_list*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 80: // insert_col_list ::= column 
            {
                ArrayList<Column> RESULT = null;
                int cleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int cright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                Column c = (Column)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new ArrayList<Column>(); RESULT.add(c); 
                CUP$Parser$result = new java_cup.runtime.Symbol(26/*insert_col_list*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 79: // insert_col_clause ::= LPAREN insert_col_list RPAREN 
            {
                ArrayList<Column> RESULT = null;
                int clleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left;
                int clright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).right;
                ArrayList<Column> cl = (ArrayList<Column>)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-1)).value;
                RESULT = cl; 
                CUP$Parser$result = new java_cup.runtime.Symbol(25/*insert_col_clause*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 78: // insert_col_clause ::= 
            {
                ArrayList<Column> RESULT = null;
                RESULT = null; 
                CUP$Parser$result = new java_cup.runtime.Symbol(25/*insert_col_clause*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 77: // insert_stmt ::= INSERT INTO table insert_col_clause VALUES LPAREN insert_val_list RPAREN 
            {
                InsertStatement RESULT = null;
                int tleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-5)).left;
                int tright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-5)).right;
                Table t = (Table)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-5)).value;
                int clleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-4)).left;
                int clright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-4)).right;
                ArrayList<Column> cl = (ArrayList<Column>)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-4)).value;
                int vlleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left;
                int vlright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).right;
                ArrayList<Object> vl = (ArrayList<Object>)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-1)).value;
                RESULT = new InsertStatement(t, cl, vl); 
                CUP$Parser$result = new java_cup.runtime.Symbol(2/*insert_stmt*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-7)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 76: // limit ::= LIMIT INT_VAL COMMA INT_VAL 
            {
                Limit RESULT = null;
                int offsetleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left;
                int offsetright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).right;
                Integer offset = (Integer)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-2)).value;
                int maxleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int maxright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                Integer max = (Integer)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new Limit(offset, max); 
                CUP$Parser$result = new java_cup.runtime.Symbol(22/*limit*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-3)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 75: // limit ::= LIMIT INT_VAL 
            {
                Limit RESULT = null;
                int maxleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int maxright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                Integer max = (Integer)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new Limit(null, max); 
                CUP$Parser$result = new java_cup.runtime.Symbol(22/*limit*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 74: // limit ::= 
            {
                Limit RESULT = null;
                RESULT = null; 
                CUP$Parser$result = new java_cup.runtime.Symbol(22/*limit*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 73: // cterm ::= STRING 
            {
                CompareTerm RESULT = null;
                int sleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int sright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                String s = (String)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new CompareTerm(s); 
                CUP$Parser$result = new java_cup.runtime.Symbol(21/*cterm*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 72: // cterm ::= REAL_VAL 
            {
                CompareTerm RESULT = null;
                int rleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int rright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                Double r = (Double)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new CompareTerm(r); 
                CUP$Parser$result = new java_cup.runtime.Symbol(21/*cterm*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 71: // cterm ::= INT_VAL 
            {
                CompareTerm RESULT = null;
                int ileft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int iright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                Integer i = (Integer)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new CompareTerm(i); 
                CUP$Parser$result = new java_cup.runtime.Symbol(21/*cterm*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 70: // cterm ::= column 
            {
                CompareTerm RESULT = null;
                int cleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int cright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                Column c = (Column)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = c; 
                CUP$Parser$result = new java_cup.runtime.Symbol(21/*cterm*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 69: // comparison ::= cterm IS NOT NULL 
            {
                Comparison RESULT = null;
                int cleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-3)).left;
                int cright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-3)).right;
                CompareTerm c = (CompareTerm)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-3)).value;
                RESULT = new Comparison(Comparison.IS_NOT_NULL, c, null); 
                CUP$Parser$result = new java_cup.runtime.Symbol(20/*comparison*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-3)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 68: // comparison ::= cterm IS NULL 
            {
                Comparison RESULT = null;
                int cleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left;
                int cright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).right;
                CompareTerm c = (CompareTerm)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-2)).value;
                RESULT = new Comparison(Comparison.IS_NULL, c, null); 
                CUP$Parser$result = new java_cup.runtime.Symbol(20/*comparison*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 67: // comparison ::= cterm CLIKE STRING 
            {
                Comparison RESULT = null;
                int lleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left;
                int lright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).right;
                CompareTerm l = (CompareTerm)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-2)).value;
                int sleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int sright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                String s = (String)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new Comparison(Comparison.CLIKE, l, new CompareTerm(s)); 
                CUP$Parser$result = new java_cup.runtime.Symbol(20/*comparison*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 66: // comparison ::= cterm LIKE STRING 
            {
                Comparison RESULT = null;
                int lleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left;
                int lright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).right;
                CompareTerm l = (CompareTerm)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-2)).value;
                int sleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int sright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                String s = (String)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new Comparison(Comparison.LIKE, l, new CompareTerm(s)); 
                CUP$Parser$result = new java_cup.runtime.Symbol(20/*comparison*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 65: // comparison ::= cterm GTEQ cterm 
            {
                Comparison RESULT = null;
                int lleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left;
                int lright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).right;
                CompareTerm l = (CompareTerm)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-2)).value;
                int rleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int rright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                CompareTerm r = (CompareTerm)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new Comparison(Comparison.GTEQ, l, r); 
                CUP$Parser$result = new java_cup.runtime.Symbol(20/*comparison*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 64: // comparison ::= cterm LTEQ cterm 
            {
                Comparison RESULT = null;
                int lleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left;
                int lright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).right;
                CompareTerm l = (CompareTerm)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-2)).value;
                int rleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int rright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                CompareTerm r = (CompareTerm)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new Comparison(Comparison.LTEQ, l, r); 
                CUP$Parser$result = new java_cup.runtime.Symbol(20/*comparison*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 63: // comparison ::= cterm GT cterm 
            {
                Comparison RESULT = null;
                int lleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left;
                int lright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).right;
                CompareTerm l = (CompareTerm)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-2)).value;
                int rleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int rright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                CompareTerm r = (CompareTerm)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new Comparison(Comparison.GT, l, r); 
                CUP$Parser$result = new java_cup.runtime.Symbol(20/*comparison*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 62: // comparison ::= cterm LT cterm 
            {
                Comparison RESULT = null;
                int lleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left;
                int lright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).right;
                CompareTerm l = (CompareTerm)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-2)).value;
                int rleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int rright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                CompareTerm r = (CompareTerm)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new Comparison(Comparison.LT, l, r); 
                CUP$Parser$result = new java_cup.runtime.Symbol(20/*comparison*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 61: // comparison ::= cterm NOTEQ cterm 
            {
                Comparison RESULT = null;
                int lleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left;
                int lright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).right;
                CompareTerm l = (CompareTerm)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-2)).value;
                int rleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int rright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                CompareTerm r = (CompareTerm)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new Comparison(Comparison.NOTEQ, l, r); 
                CUP$Parser$result = new java_cup.runtime.Symbol(20/*comparison*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 60: // comparison ::= cterm EQ cterm 
            {
                Comparison RESULT = null;
                int lleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left;
                int lright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).right;
                CompareTerm l = (CompareTerm)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-2)).value;
                int rleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int rright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                CompareTerm r = (CompareTerm)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new Comparison(Comparison.EQ, l, r); 
                CUP$Parser$result = new java_cup.runtime.Symbol(20/*comparison*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 59: // conditional_primary ::= LPAREN conditional_expression RPAREN 
            {
                ConditionalExpression RESULT = null;
                int eleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left;
                int eright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).right;
                ConditionalExpression e = (ConditionalExpression)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-1)).value;
                RESULT = e; 
                CUP$Parser$result = new java_cup.runtime.Symbol(19/*conditional_primary*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 58: // conditional_primary ::= comparison 
            {
                ConditionalExpression RESULT = null;
                int cleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int cright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                Comparison c = (Comparison)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = c; 
                CUP$Parser$result = new java_cup.runtime.Symbol(19/*conditional_primary*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 57: // conditional_factor ::= NOT conditional_primary 
            {
                ConditionalExpression RESULT = null;
                int pleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int pright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                ConditionalExpression p = (ConditionalExpression)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new NotExpression(p); 
                CUP$Parser$result = new java_cup.runtime.Symbol(18/*conditional_factor*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 56: // conditional_factor ::= conditional_primary 
            {
                ConditionalExpression RESULT = null;
                int pleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int pright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                ConditionalExpression p = (ConditionalExpression)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = p; 
                CUP$Parser$result = new java_cup.runtime.Symbol(18/*conditional_factor*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 55: // conditional_term ::= conditional_term AND conditional_factor 
            {
                ConditionalExpression RESULT = null;
                int leftleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left;
                int leftright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).right;
                ConditionalExpression left = (ConditionalExpression)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-2)).value;
                int rightleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int rightright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                ConditionalExpression right = (ConditionalExpression)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new AndExpression(left, right); 
                CUP$Parser$result = new java_cup.runtime.Symbol(17/*conditional_term*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 54: // conditional_term ::= conditional_factor 
            {
                ConditionalExpression RESULT = null;
                int fleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int fright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                ConditionalExpression f = (ConditionalExpression)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = f; 
                CUP$Parser$result = new java_cup.runtime.Symbol(17/*conditional_term*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 53: // conditional_expression ::= conditional_expression OR conditional_term 
            {
                ConditionalExpression RESULT = null;
                int leftleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left;
                int leftright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).right;
                ConditionalExpression left = (ConditionalExpression)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-2)).value;
                int rightleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int rightright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                ConditionalExpression right = (ConditionalExpression)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new OrExpression(left, right); 
                CUP$Parser$result = new java_cup.runtime.Symbol(16/*conditional_expression*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 52: // conditional_expression ::= conditional_term 
            {
                ConditionalExpression RESULT = null;
                int tleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int tright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                ConditionalExpression t = (ConditionalExpression)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = t; 
                CUP$Parser$result = new java_cup.runtime.Symbol(16/*conditional_expression*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 51: // where_clause ::= WHERE conditional_expression 
            {
                ConditionalExpression RESULT = null;
                int celeft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int ceright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                ConditionalExpression ce = (ConditionalExpression)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = ce; 
                CUP$Parser$result = new java_cup.runtime.Symbol(15/*where_clause*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 50: // where_clause ::= 
            {
                ConditionalExpression RESULT = null;
                RESULT = null; 
                CUP$Parser$result = new java_cup.runtime.Symbol(15/*where_clause*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 49: // range_variable ::= AS ID 
            {
                String RESULT = null;
                int varleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int varright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                String var = (String)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = var; 
                CUP$Parser$result = new java_cup.runtime.Symbol(33/*range_variable*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 48: // range_variable ::= ID 
            {
                String RESULT = null;
                int varleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int varright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                String var = (String)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = var; 
                CUP$Parser$result = new java_cup.runtime.Symbol(33/*range_variable*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 47: // range_variable ::= 
            {
                String RESULT = null;
                RESULT = null; 
                CUP$Parser$result = new java_cup.runtime.Symbol(33/*range_variable*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 46: // table ::= ID 
            {
                Table RESULT = null;
                int nameleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int nameright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                String name = (String)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new Table(name); 
                CUP$Parser$result = new java_cup.runtime.Symbol(10/*table*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 45: // from_item ::= table range_variable 
            {
                Table RESULT = null;
                int tleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left;
                int tright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).right;
                Table t = (Table)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-1)).value;
                int rleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int rright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                String r = (String)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = t; 
                if (r != null)
                    RESULT.setRangeVariable(r);
                
                CUP$Parser$result = new java_cup.runtime.Symbol(11/*from_item*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 44: // from_list ::= from_list COMMA from_item 
            {
                ArrayList<Table> RESULT = null;
                int flleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left;
                int flright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).right;
                ArrayList<Table> fl = (ArrayList<Table>)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-2)).value;
                int ileft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int iright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                Table i = (Table)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = fl; RESULT.add(i); 
                CUP$Parser$result = new java_cup.runtime.Symbol(23/*from_list*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 43: // from_list ::= from_item 
            {
                ArrayList<Table> RESULT = null;
                int ileft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int iright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                Table i = (Table)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new ArrayList<Table>(); RESULT.add(i); 
                CUP$Parser$result = new java_cup.runtime.Symbol(23/*from_list*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 42: // column_val ::= NULL 
            {
                Object RESULT = null;
                RESULT = null; 
                CUP$Parser$result = new java_cup.runtime.Symbol(31/*column_val*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 41: // column_val ::= STRING 
            {
                Object RESULT = null;
                int sleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int sright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                String s = (String)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = s; 
                CUP$Parser$result = new java_cup.runtime.Symbol(31/*column_val*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 40: // column_val ::= REAL_VAL 
            {
                Object RESULT = null;
                int rleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int rright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                Double r = (Double)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = r; 
                CUP$Parser$result = new java_cup.runtime.Symbol(31/*column_val*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 39: // column_val ::= INT_VAL 
            {
                Object RESULT = null;
                int ileft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int iright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                Integer i = (Integer)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = i; 
                CUP$Parser$result = new java_cup.runtime.Symbol(31/*column_val*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 38: // column ::= table DOT ID 
            {
                Column RESULT = null;
                int tleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left;
                int tright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).right;
                Table t = (Table)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-2)).value;
                int nameleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int nameright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                String name = (String)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new Column(name, t); 
                CUP$Parser$result = new java_cup.runtime.Symbol(12/*column*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 37: // column ::= ID 
            {
                Column RESULT = null;
                int nameleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int nameright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                String name = (String)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new Column(name); 
                CUP$Parser$result = new java_cup.runtime.Symbol(12/*column*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 36: // select_item ::= STAR 
            {
                Object RESULT = null;
                RESULT = SelectStatement.STAR; 
                CUP$Parser$result = new java_cup.runtime.Symbol(32/*select_item*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 35: // select_item ::= column_val 
            {
                Object RESULT = null;
                int vleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int vright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                Object v = (Object)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = v; 
                CUP$Parser$result = new java_cup.runtime.Symbol(32/*select_item*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 34: // select_item ::= column 
            {
                Object RESULT = null;
                int cleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int cright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                Column c = (Column)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = c; 
                CUP$Parser$result = new java_cup.runtime.Symbol(32/*select_item*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 33: // select_list ::= select_list COMMA select_item 
            {
                ArrayList<Object> RESULT = null;
                int slleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left;
                int slright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).right;
                ArrayList<Object> sl = (ArrayList<Object>)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-2)).value;
                int ileft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int iright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                Object i = (Object)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = sl; RESULT.add(i); 
                CUP$Parser$result = new java_cup.runtime.Symbol(28/*select_list*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 32: // select_list ::= select_item 
            {
                ArrayList<Object> RESULT = null;
                int ileft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int iright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                Object i = (Object)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new ArrayList<Object>(); RESULT.add(i); 
                CUP$Parser$result = new java_cup.runtime.Symbol(28/*select_list*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 31: // distinct_specified ::= DISTINCT 
            {
                Boolean RESULT = null;
                RESULT = new Boolean(true);  
                CUP$Parser$result = new java_cup.runtime.Symbol(30/*distinct_specified*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 30: // distinct_specified ::= ALL 
            {
                Boolean RESULT = null;
                RESULT = new Boolean(false); 
                CUP$Parser$result = new java_cup.runtime.Symbol(30/*distinct_specified*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 29: // distinct_specified ::= 
            {
                Boolean RESULT = null;
                RESULT = new Boolean(false); 
                CUP$Parser$result = new java_cup.runtime.Symbol(30/*distinct_specified*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 28: // select_stmt ::= SELECT distinct_specified select_list FROM from_list where_clause limit 
            {
                SelectStatement RESULT = null;
                int dleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-5)).left;
                int dright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-5)).right;
                Boolean d = (Boolean)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-5)).value;
                int slleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-4)).left;
                int slright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-4)).right;
                ArrayList<Object> sl = (ArrayList<Object>)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-4)).value;
                int flleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left;
                int flright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).right;
                ArrayList<Table> fl = (ArrayList<Table>)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-2)).value;
                int wleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left;
                int wright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).right;
                ConditionalExpression w = (ConditionalExpression)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-1)).value;
                int lleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int lright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                Limit l = (Limit)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new SelectStatement(sl, fl, w, l, d); 
                CUP$Parser$result = new java_cup.runtime.Symbol(1/*select_stmt*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-6)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 27: // col_def_opts ::= PRIMARY KEY NOT NULL 
            {
                ColumnOptions RESULT = null;
                RESULT = new ColumnOptions(true, true); 
                CUP$Parser$result = new java_cup.runtime.Symbol(14/*col_def_opts*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-3)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 26: // col_def_opts ::= NOT NULL PRIMARY KEY 
            {
                ColumnOptions RESULT = null;
                RESULT = new ColumnOptions(true, true); 
                CUP$Parser$result = new java_cup.runtime.Symbol(14/*col_def_opts*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-3)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 25: // col_def_opts ::= PRIMARY KEY 
            {
                ColumnOptions RESULT = null;
                RESULT = new ColumnOptions(false, true); 
                CUP$Parser$result = new java_cup.runtime.Symbol(14/*col_def_opts*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 24: // col_def_opts ::= NOT NULL 
            {
                ColumnOptions RESULT = null;
                RESULT = new ColumnOptions(true, false); 
                CUP$Parser$result = new java_cup.runtime.Symbol(14/*col_def_opts*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 23: // col_def_opts ::= 
            {
                ColumnOptions RESULT = null;
                RESULT = null; 
                CUP$Parser$result = new java_cup.runtime.Symbol(14/*col_def_opts*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 22: // col_def ::= column REAL col_def_opts 
            {
                Column RESULT = null;
                int cleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left;
                int cright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).right;
                Column c = (Column)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-2)).value;
                int oleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int oright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                ColumnOptions o = (ColumnOptions)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = c; RESULT.setType(Column.REAL); RESULT.applyOpts(o); 
                CUP$Parser$result = new java_cup.runtime.Symbol(13/*col_def*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 21: // col_def ::= column VARCHAR LPAREN INT_VAL RPAREN col_def_opts 
            {
                Column RESULT = null;
                int cleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-5)).left;
                int cright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-5)).right;
                Column c = (Column)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-5)).value;
                int ileft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left;
                int iright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).right;
                Integer i = (Integer)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-2)).value;
                int oleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int oright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                ColumnOptions o = (ColumnOptions)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = c; RESULT.setType(Column.VARCHAR); RESULT.setLength(i);
                RESULT.applyOpts(o); 
                CUP$Parser$result = new java_cup.runtime.Symbol(13/*col_def*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-5)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 20: // col_def ::= column CHAR LPAREN INT_VAL RPAREN col_def_opts 
            {
                Column RESULT = null;
                int cleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-5)).left;
                int cright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-5)).right;
                Column c = (Column)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-5)).value;
                int ileft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left;
                int iright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).right;
                Integer i = (Integer)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-2)).value;
                int oleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int oright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                ColumnOptions o = (ColumnOptions)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = c; RESULT.setType(Column.CHAR); RESULT.setLength(i);
                RESULT.applyOpts(o); 
                CUP$Parser$result = new java_cup.runtime.Symbol(13/*col_def*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-5)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 19: // col_def ::= column INTEGER col_def_opts 
            {
                Column RESULT = null;
                int cleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left;
                int cright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).right;
                Column c = (Column)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-2)).value;
                int oleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int oright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                ColumnOptions o = (ColumnOptions)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = c; RESULT.setType(Column.INTEGER); RESULT.applyOpts(o); 
                CUP$Parser$result = new java_cup.runtime.Symbol(13/*col_def*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 18: // col_def_list ::= col_def_list COMMA col_def 
            {
                ArrayList<Column> RESULT = null;
                int cdlleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left;
                int cdlright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).right;
                ArrayList<Column> cdl = (ArrayList<Column>)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-2)).value;
                int cleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int cright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                Column c = (Column)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = cdl; RESULT.add(c); 
                CUP$Parser$result = new java_cup.runtime.Symbol(24/*col_def_list*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 17: // col_def_list ::= col_def 
            {
                ArrayList<Column> RESULT = null;
                int cleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int cright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                Column c = (Column)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new ArrayList<Column>(); RESULT.add(c); 
                CUP$Parser$result = new java_cup.runtime.Symbol(24/*col_def_list*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 16: // create_stmt ::= CREATE TABLE table LPAREN col_def_list RPAREN 
            {
                CreateStatement RESULT = null;
                int tleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-3)).left;
                int tright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-3)).right;
                Table t = (Table)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-3)).value;
                int cdlleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left;
                int cdlright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).right;
                ArrayList<Column> cdl = (ArrayList<Column>)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-1)).value;
                RESULT = new CreateStatement(t, cdl); 
                CUP$Parser$result = new java_cup.runtime.Symbol(5/*create_stmt*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-5)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 15: // optional_work ::= WORK 
            {
                Object RESULT = null;
                
                CUP$Parser$result = new java_cup.runtime.Symbol(34/*optional_work*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 14: // optional_work ::= 
            {
                Object RESULT = null;
                
                CUP$Parser$result = new java_cup.runtime.Symbol(34/*optional_work*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 13: // rollback_stmt ::= ROLLBACK optional_work 
            {
                RollbackStatement RESULT = null;
                RESULT = new RollbackStatement(); 
                CUP$Parser$result = new java_cup.runtime.Symbol(9/*rollback_stmt*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 12: // commit_stmt ::= COMMIT optional_work 
            {
                CommitStatement RESULT = null;
                RESULT = new CommitStatement(); 
                CUP$Parser$result = new java_cup.runtime.Symbol(8/*commit_stmt*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 11: // begin_stmt ::= BEGIN WORK 
            {
                BeginStatement RESULT = null;
                RESULT = new BeginStatement(); 
                CUP$Parser$result = new java_cup.runtime.Symbol(7/*begin_stmt*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 10: // drop_stmt ::= DROP TABLE table 
            {
                DropStatement RESULT = null;
                int tleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).left;
                int tright = ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right;
                Table t = (Table)((java_cup.runtime.Symbol) CUP$Parser$stack.peek()).value;
                RESULT = new DropStatement(t); 
                CUP$Parser$result = new java_cup.runtime.Symbol(6/*drop_stmt*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-2)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 9: // sql_stmt ::= rollback_stmt SEMICOLON 
            {
                SQLStatement RESULT = null;
                int rleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left;
                int rright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).right;
                RollbackStatement r = (RollbackStatement)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-1)).value;
                RESULT = r; 
                CUP$Parser$result = new java_cup.runtime.Symbol(0/*sql_stmt*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 8: // sql_stmt ::= commit_stmt SEMICOLON 
            {
                SQLStatement RESULT = null;
                int cleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left;
                int cright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).right;
                CommitStatement c = (CommitStatement)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-1)).value;
                RESULT = c; 
                CUP$Parser$result = new java_cup.runtime.Symbol(0/*sql_stmt*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 7: // sql_stmt ::= begin_stmt SEMICOLON 
            {
                SQLStatement RESULT = null;
                int bleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left;
                int bright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).right;
                BeginStatement b = (BeginStatement)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-1)).value;
                RESULT = b; 
                CUP$Parser$result = new java_cup.runtime.Symbol(0/*sql_stmt*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 6: // sql_stmt ::= drop_stmt SEMICOLON 
            {
                SQLStatement RESULT = null;
                int dleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left;
                int dright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).right;
                DropStatement d = (DropStatement)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-1)).value;
                RESULT = d; 
                CUP$Parser$result = new java_cup.runtime.Symbol(0/*sql_stmt*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 5: // sql_stmt ::= create_stmt SEMICOLON 
            {
                SQLStatement RESULT = null;
                int cleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left;
                int cright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).right;
                CreateStatement c = (CreateStatement)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-1)).value;
                RESULT = c; 
                CUP$Parser$result = new java_cup.runtime.Symbol(0/*sql_stmt*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 4: // sql_stmt ::= delete_stmt SEMICOLON 
            {
                SQLStatement RESULT = null;
                int dleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left;
                int dright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).right;
                DeleteStatement d = (DeleteStatement)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-1)).value;
                RESULT = d; 
                CUP$Parser$result = new java_cup.runtime.Symbol(0/*sql_stmt*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 3: // sql_stmt ::= update_stmt SEMICOLON 
            {
                SQLStatement RESULT = null;
                int uleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left;
                int uright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).right;
                UpdateStatement u = (UpdateStatement)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-1)).value;
                RESULT = u; 
                CUP$Parser$result = new java_cup.runtime.Symbol(0/*sql_stmt*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 2: // sql_stmt ::= insert_stmt SEMICOLON 
            {
                SQLStatement RESULT = null;
                int ileft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left;
                int iright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).right;
                InsertStatement i = (InsertStatement)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-1)).value;
                RESULT = i; 
                CUP$Parser$result = new java_cup.runtime.Symbol(0/*sql_stmt*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 1: // $START ::= sql_stmt EOF 
            {
                Object RESULT = null;
                int start_valleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left;
                int start_valright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).right;
                SQLStatement start_val = (SQLStatement)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-1)).value;
                RESULT = start_val;
                CUP$Parser$result = new java_cup.runtime.Symbol(0/*$START*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            /* ACCEPT */
            CUP$Parser$parser.done_parsing();
            return CUP$Parser$result;
            
            /*. . . . . . . . . . . . . . . . . . . .*/
            case 0: // sql_stmt ::= select_stmt SEMICOLON 
            {
                SQLStatement RESULT = null;
                int sleft = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left;
                int sright = ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).right;
                SelectStatement s = (SelectStatement)((java_cup.runtime.Symbol) CUP$Parser$stack.elementAt(CUP$Parser$top-1)).value;
                RESULT = s; 
                CUP$Parser$result = new java_cup.runtime.Symbol(0/*sql_stmt*/, ((java_cup.runtime.Symbol)CUP$Parser$stack.elementAt(CUP$Parser$top-1)).left, ((java_cup.runtime.Symbol)CUP$Parser$stack.peek()).right, RESULT);
            }
            return CUP$Parser$result;
            
            /* . . . . . .*/
            default:
                throw new Exception(
                                    "Invalid action number found in internal parse table");
                
        }
    }
}

