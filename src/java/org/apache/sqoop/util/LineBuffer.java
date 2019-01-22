package org.apache.sqoop.util;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/* Buffer up lines of text until buffer is full. 
 * This helper class is just to support a test to 
 * see if the copyIn mapper for postgres is unbuffered
 *
 * */
public class LineBuffer{
    public static final Log LOG = LogFactory.getLog(LineBuffer.class.getName());

    private StringBuilder sb=new StringBuilder();
    private static int MAXLEN=100000000;
    //private static int MAXLEN=50000000;

    public void clear(){ sb.setLength(0); }
    public int length(){return sb.length();}
    public boolean append(String s){
//	LOG.debug(s);
	if (sb.length()+s.length()+1>MAXLEN){return false;}
	sb.append(s);
	sb.append("\n");
	return true;	
    }
    public String toString(){return sb.toString();}
    public byte[] getBytes() {
	try {
	//LOG.debug("returning "+new String(sb.toString().getBytes("UTF-8")));
	return sb.toString().getBytes("UTF-8");
	}catch(Exception e){e.printStackTrace();return null;}
    }
}
