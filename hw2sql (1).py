#!/usr/bin/env python
# coding: utf-8

import sys
import pyspark
import string

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import *

if __name__ == "__main__":

    sc = SparkContext()

    spark = SparkSession \
        .builder \
        .appName("hw2sql") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    sqlContext = SQLContext(spark)

    # get command-line arguments
    inFile = sys.argv[1]
    supp = sys.argv[2]
    conf = sys.argv[3]
    prot = sys.argv[4]

    print ("Executing HW2SQL with input from " + inFile + ", support=" +supp + ", confidence=" + conf + ", protection=" + prot)

    pp_schema = StructType([
            StructField("uid", IntegerType(), True),
            StructField("attr", StringType(), True),
            StructField("val", IntegerType(), True)])

    Pro_Publica = sqlContext.read.format('csv').options(header=False).schema(pp_schema).load(inFile)
    Pro_Publica.createOrReplaceTempView("Pro_Publica")
    sqlContext.cacheTable("Pro_Publica")
    spark.sql("select count(*) from Pro_Publica").show()

    # compute frequent itemsets of size 1, store in F1(attr, val)
    query = "select attr, val, count(*) as supp \
               from Pro_Publica \
              group by attr, val \
             having count(*) >= "  + str(supp);
    F1 = spark.sql(query);
    F1.createOrReplaceTempView("F1")
    F1.show()
    
    query2 = "select A.attr as attr1,B.attr as attr2,A.val as val1, B.val as val2 \
    from (select uid, attr, val from Pro_Publica ) as A  \
    , (select uid, attr, val from Pro_Publica where attr='vdecile') as B \
    where A.uid=B.uid and A.attr!='vdecile'"                     
    F2 = spark.sql(query2)
    F2.createOrReplaceTempView("F2")
    F2.show()
    query3 = "select F2.attr1,F2.val1,F2.attr2,F2.val2, count(*) as supp \
             from F2 \
             group by F2.attr1, F2.val1, F2.attr2, F2.val2 \
             having count(*) >= "+str(supp);                     
    F3 = spark.sql(query3)
    F3.createOrReplaceTempView("F3")
    F3.show()
    query4 = "select F3.attr1,F3.val1,F3.attr2,F3.val2, F3.supp, (F3.supp/F1.supp) as conf from F1,F3 where F1.attr=F3.attr1 and F1.val=F3.val1 and (F3.supp/F1.supp)>= "+str(conf);   
    F4 = spark.sql(query4)
    F4.createOrReplaceTempView("F4")
    F4.show()
    

    # YOUR SparkSQL CODE GOES HERE
    # You may use any valid SQL query, and store the output in intermediate temporary views
    # Output must go into R2, R3 and PD_R3 as stated below.  Do not change the format of the output
    # on the last three lines.
         
    # Compute R2, as described in the homework specification
    # R2(attr1, val1, attr2, val2, supp, conf)
    query = "select F4.attr1,F4.val1,F4.attr2,F4.val2, F4.supp, F4.conf from F4 order by F4.attr1, F4.val1, F4.conf"
    R2 = spark.sql(query)
    R2.createOrReplaceTempView("R2")
    R2.show()
    
    
    query5 = "select A.uid,A.attr as attr1,A.val as val1,C.attr as attr2,C.val as val2 \
    from (select uid, attr, val from Pro_Publica ) as A  ,(select uid, attr, val from Pro_Publica) as C \
     where A.uid=C.uid and A.attr<C.attr"                     
    F5 = spark.sql(query5)
    F5.createOrReplaceTempView("F5") 
    F5.show()
    query6 = "select F5.attr1,F5.val1,F5.attr2,F5.val2, count(*) as supp \
             from F5 \
             group by F5.attr1, F5.val1, F5.attr2, F5.val2 \
             having count(*) >= "+str(supp);                     
    F6 = spark.sql(query6)
    F6.createOrReplaceTempView("F6")
    F6.show()
    
    query7="select uid2, attr1, val1,attr2,val2,B.attr as attr3, B.val as val3 \
    from (select F5.uid as uid2,F5.attr1 as attr1,F5.val1 as val1,F5.attr2 as attr2,F5.val2 as val2 from F5 ) as A  \
    , (select uid, attr, val from Pro_Publica where attr='vdecile') as B \
    where B.uid=uid2";
    F7 = spark.sql(query7)
    F7.createOrReplaceTempView("F7")
    F7.show()
    
    query8 = "select F7.attr1,F7.val1,F7.attr2,F7.val2,F7.attr3, F7.val3, count(*) as supp \
    from F7 \
    group by F7.attr1, F7.val1,F7.attr2, F7.val2, F7.attr3, F7.val3 \
    having count(*) >= "+str(supp)                  
    F8 = spark.sql(query8)
    F8.createOrReplaceTempView("F8")
    F8.show()
    # MORE OF YOUR SparkSQL CODE GOES HERE
    
    query9 = "select F8.attr1,F8.val1,F8.attr2,F8.val2,F8.attr3, F8.val3, F8.supp, (F8.supp/F6.supp) as conf from F6,F8 where F6.attr1=F8.attr1 and F6.val1=F8.val1 and F6.attr2=F8.attr2 and (F8.supp/F6.supp)<1 and F6.val2=F8.val2  and (F8.supp/F6.supp)>= "+str(conf);                     
    F9 = spark.sql(query9)
    F9.createOrReplaceTempView("F9")
    F9.show()
    # Compute R3, as described in the homework specification
    # R3(attr1, val1, attr2, val2, attr3, val3, supp, conf)
    query = "select F9.attr1,F9.val1,F9.attr2,F9.val2,F9.attr3, F9.val3, F9.supp, F9.conf from F9 order by F9.attr1, F9.val1, F9.attr2, F9.conf"   
    R3 = spark.sql(query)
    R3.createOrReplaceTempView("R3")
    R3.show()
    
    query10 = "select R3.attr1,R3.val1,R3.attr2,R3.val2,R3.attr3, R3.val3, R3.supp, R3.conf from R3 where R3.attr2='race'";                     
    F10 = spark.sql(query10)
    F10.createOrReplaceTempView("F10")
    F10.show()
    
    query11 = "select R3.attr1,R3.val1,R3.attr2,R3.val2,R3.attr3, R3.val3, R3.supp, R3.conf, (R3.conf/R2.conf) as prot from R3, R2 where R3.attr2='race' and R3.attr1=R2.attr1 and R3.attr3=R2.attr2 and R3.val1=R2.val1 and R3.val3=R2.val2 and (R3.conf/R2.conf)>= "+str(prot);                     
    F11 = spark.sql(query11)
    F11.createOrReplaceTempView("F11")
    F11.show()
        
    # MORE OF YOUR SparkSQL CODE GOES HERE

    # Compute PD_R3, as described in the homework specification
    # PD_R3(attr1, val1, attr2, val2, attr3, val3, supp, conf, prot)
    query = "select F11.attr1,F11.val1,F11.attr2,F11.val2,F11.attr3, F11.val3, F11.supp, F11.conf, F11.prot from F11 order by F11.attr1, F11.val1, F11.prot"   
    PD_R3 = spark.sql(query)
    PD_R3.createOrReplaceTempView("PD_R3")
    PD_R3.show()

    R2.select(format_string('%s,%s,%s,%s,%d,%.2f',R2.attr1,R2.val1,R2.attr2,R2.val2,R2.supp,R2.conf)).write.save("r2.out",format="text")
    R3.select(format_string('%s,%s,%s,%s,%s,%s,%d,%.2f',R3.attr1,R3.val1,R3.attr2,R3.val2,R3.attr3,R3.val3,R3.supp,R3.conf)).write.save("r3.out",format="text")
    PD_R3.select(format_string('%s,%s,%s,%s,%s,%s,%d,%.2f,%.2f',PD_R3.attr1,PD_R3.val1,PD_R3.attr2,PD_R3.val2,PD_R3.attr3,PD_R3.val3,PD_R3.supp,PD_R3.conf,PD_R3.prot)).write.save("pd-r3.out",format="text")
    sc.stop()

