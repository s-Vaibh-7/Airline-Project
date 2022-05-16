from pyspark.sql import SparkSession
from common.readdatautil import ReadDataUtil
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("Airline Project").getOrCreate()


    rdu = ReadDataUtil()

    airlineschema = StructType([StructField("airline_id", IntegerType()),
                                StructField("name", StringType()),
                                StructField("alias", StringType()),
                                StructField("iata", StringType()),
                                StructField("icao", StringType()),
                                StructField("callsign", StringType()),
                                StructField("country", StringType()),
                                StructField("active", StringType())])

    df1 = rdu.readCsv(spark=spark,path=r"C:\BRAINWORKS\Big_Data\Project\airline.csv",schema=airlineschema)
    # df1.printSchema()
    # df1.show()


    airportschema = StructType([StructField("airport_id", IntegerType()),
                                StructField("name", StringType()),
                                StructField("city", StringType()),
                                StructField("country", StringType()),
                                StructField("iata", StringType()),
                                StructField("icao", StringType()),
                                StructField("latitude", FloatType()),
                                StructField("longitude", FloatType()),
                                StructField("altitude", StringType()),
                                StructField("timezone", StringType()),
                                StructField("dst", StringType()),
                                StructField("type", StringType()),
                                StructField("source", StringType())])
    #
    df2 = rdu.readCsv(spark=spark,path=r"C:\BRAINWORKS\Big_Data\Project\airport.csv", schema=airportschema, header=True)
    # df2.printSchema()
    # df2.show()

    df3 = rdu.readCsv(spark=spark,path=r"C:\BRAINWORKS\Big_Data\Project\plane.csv", inferschema=True, header=True, sep='')
    # df3.show()

    df4 = spark.read.parquet(r"C:\BRAINWORKS\Big_Data\Project\routes.snappy.parquet", inferSchema=True)
    # df4.printSchema()
    # df4.show()

    #### 1) in any of your input file if you are getting \N or null values in your column and that column is of string type then put default value as "Uncommon" and if column is of  type integer put -1
    #
    # airdf = df1.fillna('(unknown)', ['iata', 'callsign', 'icao', 'country'])
    # airdf.show()


    # _-------------sql--------------------#

    # df1.createOrReplaceTempView("airline")
    # spark.sql("select airline_id,name,replace(alias,'\\N','(unknown)')alias,nvl(iata,'(unknown)')iata,nvl(icao,'(unknown)')\
    #     icao,nvl(callsign,'(unknown)')callsign,nvl(country,'(unknown)')country,active from airline a ").show()



    # -------for planes-----------------

    # planenull = df3.select('*').replace(r'\N',value='(unknown)',subset=['ICAO code'])
    # planenull = df3.withColumn("ICAO code", regexp_replace(df3["ICAO code"], r"\\N", "(unknown)"))
    # planenull.show()

    # ----------for routes-------------
    # routenull = df4.fillna('(unknown)', 'codeshare') \
    #     .withColumn("dest_airport_id", regexp_replace(df4["dest_airport_id"], r"\\N", "(unknown)")) \
    #     .withColumn("src_airport_id", regexp_replace(df4["src_airport_id"], r"\\N", "(unknown)")) \
    #     .withColumn("airline_id", regexp_replace(df4["airline_id"], r"\\N", "(unknown)"))
    # routenull.show()

    # #### 2) find the country name which is having both airlines and airport
    #
    # # ---1st method
    #
    output = df1.join(df2, df1.country == df2.country, "inner") \
        .select(df1.country.alias("country"), df2.name.alias("airport_name"), df1.name.alias("Airline_name"))

    # output.show()
    # output.printSchema()
    #
    # # ---2nd type
    #
    # airpdf = output.groupBy("country").agg(
    #     concat_ws(", ", collect_list("Airport_name")) \
    #         .alias("Airport_Name"))
    # airpdf.show()
    # #
    # airldf = output.groupBy("country").agg(
    #     concat_ws(", ", collect_list("Airline_name")) \
    #         .alias("Airline_Name"))
    # airldf.show()
    # #
    # joidf = airpdf.join(airldf, airpdf.country == airldf.country, "inner") \
    #     .select(airpdf["country"], "Airport_Name", "Airline_Name")
    # joidf.show()

    #
    # # # 3rd method-----------SQL--------
    df2.createOrReplaceTempView("airport")
    df1.createOrReplaceTempView("airline")
    # spark.sql("select a.name as airlinename, b.name as airportname ,a.country from airline a join airport b on a.country == b.country ").show()

    #
    # ## 3) get the airlines details like name,id,which is has been taken takeoff more than 3 times from same airport
    # #
    # takeoff = df4.join(df1, df4.airline_id == df1.airline_id, "inner")\
    #     .join(df2,df4.src_airport == df2.iata).groupBy(
    #     df1.airline_id, df1.name,
    #     df4.src_airport_id,
    #     df4.src_airport).count()
    # takeoff.where(col("count") > 3).distinct().show()
    #
    # # -----sql-----
    #
    df4.createOrReplaceTempView("route")
    df3.createOrReplaceTempView("plane")
    # #
    # # # spark.sql("select r.airline_id,a.name,r.src_airport_id,r.")
    #
    # Count = spark.sql('select a.name, src_airport, b.airline_id, count(*) from airline a '
    #                   'join route b '
    #                   'on a.airline_id = b.airline_id '
    #                   'group by a.name, b.src_airport, b.airline_id having count(*)>3')
    # Count.show()
    # #
    # filter = df4.select(col('src_airport_id'), col('airline_id').cast('int').alias("airline_id"),
    #                        col('src_airport')) \
    #     .groupBy('src_airport_id', 'src_airport', 'airline_id').count() \
    #     .where(r"src_airport_id != '\N' ") \
    #     .where('count > 3')
    #
    # filter.show()
    # # # filter.printSchema()
    # #
    # condition1 = [filter['airline_id'] == df1['airline_id']]
    # condition2 = [filter['src_airport'] == df2['iata']]
    #
    # Result = filter.join(df1, on=condition1) \
    #     .select(
    #     df1.name.alias('Airline_name'),
    #     df1.airline_id,
    #     filter['src_airport']) \
    #     .join(df2, on=condition2) \
    #     .select(
    #     'Airline_name',
    #     df1.airline_id,
    #     df2.name.alias('Airport_Name')).distinct()
    # #
    # print(Result.count())
    # Result.show()
    # #
    # # 4) get airport details which has minimum number of takeoffs and landings
    # #
    # #
    # a = df4.groupBy(col('src_airport'), col('dest_airport')).count()
    # b = df4.groupBy(col('src_airport'), col('dest_airport')).count().orderBy(asc('count')).select('count').head(1)[
    #     0]['count']
    #
    # details = df2.join(a, df2.iata == a.src_airport, 'inner').select(
    #     [df2.airport_id, df2.name, df2.city, a.src_airport, a.dest_airport, a['count']]) \
    #     .filter(a['count'] == b)
    #
    # details.show()
    # print(details.count())
    #
    # # ----sql
    # query = f"""(select a.name,a.airport_id,b.src_airport,b.dest_airport,count(*) from airport a
    #             join route b on (a.iata = b.src_airport)
    #             group by a.name,a.airport_id,b.src_airport,b.dest_airport)
    #              order by count(*) asc"""
    # query = f"""select name,airport_id,src_airport,dest_airport,counts from (select a.name,a.airport_id,
    #             b.src_airport,b.dest_airport,count(*) as counts,row_number()
    #             over (order by count(*) asc) as r1
    #             from airport a join route b on (a.Iata = b.src_airport)
    #             group by a.name,a.airport_id,b.src_airport,b.dest_airport
    #             order by a.name)"""
    # spark.sql(query).show()
    #
    # 5) get airport details which is having maximum number of takeoffs and landings

    a = df4.groupBy(col('src_airport'), col('src_airport_id')).count() \
        .select('src_airport', 'src_airport_id', col('count').alias('takeoffs'))
    b = df4.groupBy(col('dest_airport'), col('dest_airport_id')).count() \
        .select('dest_airport', 'dest_airport_id', col('count').alias('landings'))
    a.show()
    b.show()
    joindf = a.join(b, a['src_airport'] == b['dest_airport'], 'full') \
        .select('src_airport', 'dest_airport', (col('takeoffs') + col('landings')).alias('total')) \
        .orderBy(desc('total'))
    joindf.show()
    #
##
    # ### ---sql-----------
    query1 = f"""select name,airport_id,src_airport,dest_airport,frequency from (select a.name,a.airport_id,
                    b.src_airport,b.dest_airport,count(*) as frequency,row_number()
                    over (order by count(*) desc) as r1
                    from airport a join route b on (a.iata = b.src_airport)
                    group by a.name,a.airport_id,b.src_airport,b.dest_airport
                    order by a.name) where r1 = 1"""
    spark.sql(query1).show()
    #
    # 6) Get the airline details which is having direct flights. details like airline id,name,source airport
    # name,destination airport name

    a = df4.filter(df4['stops'] == 0)
    # a.show()

    airdet = df1.join(a, df1.airline_id == a.airline_id,'inner').\
        select(df1.airline_id,df1.name,a.src_airport,a.dest_airport)
    airdet.show()

    spark.sql("select a.*, b.src_airport, b.dest_airport, b.stops from airline a \
              inner join route b\
              on a.airline_id = b.airline_id\
              where b.stops = 0").show()

    # spark.sql("select a.airline_id, a.name,")
    #
    # spark.sql("select airline_id from route where stops = 0").show()