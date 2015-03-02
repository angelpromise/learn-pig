/**
*打印输入的数据的日志；
*log.Info(input:Tuple=>)
*/
/**
*data:
*
*/
register learn-pig.jar //注册function函数；
A = load 'student_data' using PigStorage(',') as (name:chararray, age:Int, data_in:float);
B = foreach A generate com.ling.learn.pig.SampleEvalFunc(name) as new_name;
Dump B;
