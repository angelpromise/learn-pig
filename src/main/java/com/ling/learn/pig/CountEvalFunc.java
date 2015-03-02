package com.ling.learn.pig;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class CountEvalFunc extends EvalFunc<Long> implements Algebraic{

	private Log log = this.getLogger();
	public Long exec(Tuple input) throws IOException {
		log.info("CountEvalFunc exec Begin=> " + input.toDelimitedString("\t"));
		long out = count(input);
		log.info("CountEvalFunc exec OUT=>" + out);
		return out;
		}
    public String getInitial() {
    	
    	return Initial.class.getName();
    	}
    public String getIntermed() {return Intermed.class.getName();}
    public String getFinal() {return Final.class.getName();}
    
    static public class Initial extends EvalFunc<Tuple> {
        public Tuple exec(Tuple input) throws IOException {
        	log.info("CountEvalFunc Initial Begin=> " + input.toDelimitedString("\t"));
        	Tuple out = TupleFactory.getInstance().newTuple(count(input));
        	log.info("CountEvalFunc Initial OUT=> " + out.toDelimitedString("\t"));
            return out;
        }
    }
    
    static public class Intermed extends EvalFunc<Tuple> {
        public Tuple exec(Tuple input) throws IOException {
        	log.info("CountEvalFunc Intermed Begin=> " + input.toDelimitedString("\t"));
        	Tuple out = TupleFactory.getInstance().newTuple(sum(input));
        	log.info("CountEvalFunc Intermed OUT=> " + out.toDelimitedString("\t"));
        	return out;
        }
    }
    
    static public class Final extends EvalFunc<Long> {
        public Long exec(Tuple input) throws IOException {
        	log.info("CountEvalFunc Final Begin=> " + input.toDelimitedString("\t"));
        	long out = sum(input);
        	log.info("CountEvalFunc Final OUT=> " + out);
        	return out;
        	}
    }
    
    @SuppressWarnings("rawtypes")
	static protected Long count(Tuple input) throws ExecException {
        Object values = input.get(0);
        if (values instanceof DataBag) return ((DataBag)values).size();
        else if (values instanceof Map) return new Long(((Map)values).size());
        return 0l;
    }
    
    static protected Long sum(Tuple input) throws ExecException, NumberFormatException {
        DataBag values = (DataBag)input.get(0);
        long sum = 0;
        for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
            Tuple t = it.next();
            sum += (Long)t.get(0);
        }
        return sum;
    }

}
