/**
*load the data to local and hdfs;
*添加数据到本地和HDFS上。
*输出：目录而不是文件，包括part-****,_SUCCESS文件。
*/
local_file = load 'data' using PigStorage(' ') as (date_time:chararray, status:chararray, message:chararray);
store local_file into '~/local_file';
store local_file into 'hdfs://192.168.11.101:8020/out/local_file';