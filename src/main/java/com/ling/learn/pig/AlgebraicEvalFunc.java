package com.ling.learn.pig;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class AlgebraicEvalFunc extends EvalFunc<Long> implements Algebraic{

	/**
	 * Map阶段：  //调用Map中记录的次数。
	 * 自定义的针对Map-Reduce中的Map输入的数据的处理。
	 * map-reduce::map => out //1 to 1;
	 * out:Tuple(Long);
	 * 输入数据：tuple:(表的数据)
	 * 输出数据：tuple:(1) or (0)
	 * @author angel
	 *
	 */
	public static class Initial extends EvalFunc<Tuple>{

		@Override
		public Tuple exec(Tuple input) throws IOException {
			if(input == null || input.size() == 0){
				return null;
			}
			return TupleFactory.getInstance().newTuple(Long.valueOf(1));
		}
		
	}
	
	/**
	 * Combine阶段： //调用Map-Reduce中Map个数的次数。
	 * 接受来自Map-Reduce中Map的输入的数据集。即：Bag数据类型。
	 * 输入数据：bag：{(1),(1),(0)}
	 * 输出输入：tuple：(1239) //combine总数。
	 * @author angel
	 *
	 */
	public static class Intermed extends EvalFunc<Tuple>{
        
		@Override
		public Tuple exec(Tuple input) throws IOException {
			return TupleFactory.getInstance().newTuple(sum(input));
		}
		
	}
	
	/**
	 * Reduce阶段： //调用一次
	 * 接受来自combine的输入数据。即：Bag数据。
	 * 输入数据：bag:{(233),(232),(233332)}
	 * 输出数据：tuple:(233322); //最终的结果。
	 * @author angel
	 *
	 */
	public static class Final extends EvalFunc<Tuple>{

		@Override
		public Tuple exec(Tuple input) throws IOException {
			return TupleFactory.getInstance().newTuple(sum(input));
		}
		
	}
	
	/**
	 * Reduce:处理方法名；
	 */
	public String getFinal() {
		return AlgebraicEvalFunc.class.getName();
	}

	/**
	 * Map：处理方法名； 类名对应的就是方法名。
	 */
	public String getInitial() {
		return AlgebraicEvalFunc.class.getName();
	}

	/**
	 * Combine:处理方法名；
	 */
	public String getIntermed() {
		return AlgebraicEvalFunc.class.getName();
	}

	@Override
	public Long exec(Tuple tuple) throws IOException {
	    if(tuple == null || tuple.size() == 0){
	    	return Long.valueOf(0);
	    }
		return Long.valueOf(1);
	}

	private static long sum(Tuple tuple) throws ExecException{
		/**
		 * Intermed : 接受来自Initial的数据，输入的数据是Map-reduce中Map的每条数据：bag：{(1),(2),(0)} 
		 * Final: 接受来自Intermed的数据，输入的数据是Combine的结果：bag:{(2333),(23334)}
		 */
		DataBag db = (DataBag) tuple.get(0);
		if(db == null || db.size() == 0){
			return 0;
		}
		
		long sum = 0;
		for(Iterator<Tuple> it = db.iterator(); it.hasNext();){
			Tuple tu = it.next();
			if(tu != null && tu.size() > 0){
				sum += (Long) tu.get(0);
			}
		}
		return sum;
	}
}
