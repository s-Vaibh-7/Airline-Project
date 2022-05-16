

class ReadDataUtil:
    # def __int__(self):


    def readCsv(self,spark,path,schema=None,inferschema=True,header=True,sep=","):
        """
        Returns new dataframe by reading provided csv file
        :param spark: spark session
        :param path: csv file or directory path
        :param schema: provide schema , required when inferschema is false
        :param inferschema: if true: detect file schema else flase: ignore auto detect schema
        :param header:
        :return:

        """
        if (inferschema is False) and (schema==None):
            raise Exception("Please provide inferscema as true else provide schema foe given input file")
        if schema == None:
            readdf = spark.read.csv(path=path,inferSchema=inferschema,header=header,sep=sep)
        else:
            readdf= spark.read.csv(path=path,schema=schema,header=header,sep=sep)
        return readdf



    # def readParquet(self,spark,path,inferschema,):
    #     """
    #
    #     :param spark:
    #     :param path:
    #     :param inferschema:
    #     :return:
    #     """
    #
    #     if (inferschema is False) :
    #         raise Exception("Please provide inferscema as true else provide schema foe given input file")
    #
    #     else:
    #         readdf1= spark.read.parquet(path=path,inferschema=True)
    #     return readdf1




