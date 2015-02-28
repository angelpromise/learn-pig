package com.ling.learn.pig;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * 简单的EvalFunction测试。
 * <p>调用内部的log来打印和记录输入的数据信息，和内部的状况。</p>
 * @author angel
 * @time 2015-2-28
 *
 */
public class SampleEvalFunc extends EvalFunc<Tuple>{

	private Log log = this.getLogger();
	private StringBuffer stb;
	
	public SampleEvalFunc(){
		super();
		this.stb = new StringBuffer();
		log.info("The Schema Fields=>" + this.getInputSchema().getFields().toString());
	}
	@Override
	public Tuple exec(Tuple tuple) throws IOException {
		if(tuple == null || tuple.size() == 0){
			return tuple;
		}
		
		//加载所有的数据。toString
		for(Object obj : tuple.getAll()){
			stb.append(obj.toString() + "=>");
		}
		
		//日志记录;
		log.info(stb.toString());
		stb.delete(0, stb.toString().length());
		
		return tuple;
	}
	
	/**
	 * 自定义的在完成的时候的数据清理。
	 */
	@Override
	public void finish() {
		super.finish();
	}
	
	/**
	 * 自定义的表输出格式。
	 */
	@Override
	public Schema outputSchema(Schema input) {
		return super.outputSchema(input);
	}
	
	
}
