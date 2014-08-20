package nexmark;
/*  
   NEXMark Generator -- Niagara Extension to XMark Data Generator

   Acknowledgements:
   The NEXMark Generator was developed using the xmlgen generator 
   from the XMark Benchmark project as a basis. The NEXMark
   generator generates streams of auction elements (bids, items
   for auctions, persons) as opposed to the auction files
   generated by xmlgen.  xmlgen was developed by Florian Waas.
   See http://www.xml-benchmark.org for information.

   Copyright (c) Dept. of  Computer Science & Engineering,
   OGI School of Science & Engineering, OHSU. All Rights Reserved.

   Permission to use, copy, modify, and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies
   of the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.

   THE AUTHORS AND THE DEPT. OF COMPUTER SCIENCE & ENGINEERING 
   AT OHSU ALLOW USE OF THIS SOFTWARE IN ITS "AS IS" CONDITION, 
   AND THEY DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES 
   WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

   This software was developed with support from NSF ITR award
   IIS0086002 and from DARPA through NAVY/SPAWAR 
   Contract No. N66001-99-1-8098.

*/

import java.nio.*;

// Think char buf was the fastest..., or faster than using the writer

class MyBuffer {
    CharBuffer cb = CharBuffer.allocate(8192);

    public void append(String s) {
        cb.put(s);
    }
    public void append(int i){
        if(i<IntToString.NUM_VALS)
            cb.put(IntToString.strings[i]);
        else
            cb.put(String.valueOf(i));	    
    }
    public void append(long l) {
        if(l<IntToString.NUM_VALS) {
            cb.put(IntToString.strings[(int)l]);
        } else {
            cb.put(String.valueOf(l));	    
        }
    }
    /*    public void append(double d) {
          cb.put(String.valueOf(d));
          }*/
    public void append(CharBuffer other) {
        cb.put(other.array(), 0, other.position());
    }
    public void clear(){
        // stringBuf.setLength(0);
        cb.clear();
    }
    public char[] array() {
        return cb.array();
    }
    // number of characters that have been put into this buffer
    public int length() {
        return cb.position();
    }
}
