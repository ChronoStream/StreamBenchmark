package stream.benchmark.nexmark;
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

import java.util.*;
import java.io.Serializable;

public class PersonGen implements Serializable{
	private static final long serialVersionUID = 1L;

    public String m_stName;
    public String m_stEmail;
    public String m_stCity;
    public String m_stProvince;
    public String m_stCountry;
    
    private Random m_rnd = new Random();
    
    public void generateValues() {
        int ifn = m_rnd.nextInt(FirstnamesGen.NUM_FIRSTNAMES);
        int iln = m_rnd.nextInt(LastnamesGen.NUM_LASTNAMES);
        int iem = m_rnd.nextInt(BigValueGen.NUM_EMAILS);
	
        m_stName=FirstnamesGen.firstnames[ifn]+" "+LastnamesGen.lastnames[iln];
        m_stEmail=LastnamesGen.lastnames[iln]+"@"+BigValueGen.emails[iem];
	

        int ict = m_rnd.nextInt(BigValueGen.NUM_CITIES); // city
        int icn = (m_rnd.nextInt(4) != 0) ? 0 :
            m_rnd.nextInt(BigValueGen.NUM_COUNTRIES); 
        int ipv = (icn == 0) ? m_rnd.nextInt(BigValueGen.NUM_PROVINCES) :
            m_rnd.nextInt(LastnamesGen.NUM_LASTNAMES);  // provinces are really states
	
        m_stCity = BigValueGen.cities[ict];
	
        if (icn == 0) {
            m_stCountry = "United States";
            m_stProvince = BigValueGen.provinces[ipv];
        } else {
            m_stCountry = BigValueGen.countries[icn];
            m_stProvince = LastnamesGen.lastnames[ipv];
        }
    }
}
