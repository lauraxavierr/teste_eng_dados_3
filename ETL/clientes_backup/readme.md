py4j.protocol.Py4JJavaError: An error occurred while calling o68.save.                                                   : org.apache.hadoop.fs.s3.S3Exception: org.jets3t.service.S3ServiceException: S3 Error Message. -- ResponseCode: 403, ResponseStatus: Forbidden, XML Error Message: <?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code><Message>User: arn:aws:iam::920126444373:user/teste-laura is not authorized to perform: s3:ListBucket on resource: "arn:aws:s3:::bucket-bronze-lauraxiz" because no identity-based policy allows the s3:ListBucket action</Message><RequestId>91TA6CNSW25X67V2</RequestId><HostId>zfOy8OeiAWVb1iEmQHK1PYncraqgzhj/NQ3f9foBW7D0qMIoaR3SdE4mUkZjfBwQ3yhxceWXb5/rQ+CVY+tcim3jFF9OVGOJ</HostId></Error>                                                       



colocar que daria para 



        df = (
            spark.read
            .schema(self.get_bronze_schema())
            .option("header", "true")
            .csv("file:///mnt/notebooks/clientes_sinteticos.csv")
            .repartition(4)  # força paralelismo para os demais workers; Dessa forma garantimos que o processamento não fique restrito a apenas 1 executor
        )


spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.drive.extraPythonPath=/mnt/etl --conf spark.executor.extraPythonPath=/mnt/etl /mnt/etl/clientes/main.py



#partição lógica; Dessa forma garantimos que o processamento não fique restrito a apenas 1 executor

comentar sobre a particao logica antes da particao fisica pedida no desafio


.partitionBy("anomesdia")
é a particao fisica

“Uso repartition para garantir paralelismo controlado e partitionBy para criar partição física no storage conforme o enunciado.”



            #melhoria futura
            #.config("spark.sql.shuffle.partitions", "4")
            #.config('spark.redaction.regex', '(?i)secret|password|token') #melhoria futura
            #.config('spark.sql.redaction.string.regex', '(?i)secret|password|token') \ #melhoria futura