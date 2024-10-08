import sys
import os

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
# NEW TO BE IMPORTED BELOW
import boto3
from boto3.s3.transfer import TransferConfig
import logging
import uuid
import hashlib
import re #regular expresions
import datetime
#
from rdflib import Graph, Namespace, URIRef, Literal, BNode
from rdflib.namespace import RDF, FOAF, SKOS, RDFS, DC, DCTERMS, XSD, DCAT

from builthub import DEFAULT_NS, DEFAULT_SET_NS, CO, ATOLD, BUILTHUB, EU27, EU28
from builthub import createBHGraph, createBHResource
from builthub import createSpatialPredicate, createHasNutsPredicate, createMeasurementPredicate #common functions
from builthub import createTemporalPredicate, createPeriodicityPredicate, createTemporalPredicateFromDates, createGeometryPredicate   #optional functions
from builthub import BuiltHubException
from builthub import GEO

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "builthubgluedb", table_name = "builthub_dataset000", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(
    database = "builthubgluedb", 
    table_name = "builthub_dataset029",
    transformation_ctx = "DataSource0"
)
## @type: ApplyMapping
## @args: [mappings = [("country", "string", "country", "string"), ("total floor space in eu (mm2)", "double", "total floor space in eu (mm2)", "float"), (" heated floor area (mm²) ", "double", "heated floor area (mm²)", "float"), ("cooled floor area (mm²)", "double", "cooled floor area (mm²)", "float"), ("average space heating consumption (kwh/m2_a)", "double", "average space heating consumption (kwh/m2_a)", "float"), ("total space heating consumption (twh/a)", "double", "total space heating consumption (twh/a)", "smallint"), ("average dhw consumption (kwh/m2_a)", "double", "average dhw consumption (kwh/m2_a)", "timestamp"), ("total dhw consumption (twh/a) ", "double", "total dhw consumption (twh/a) ", "string"), ("average space cooling consumption (kwh/m2_a)", "double", "average space cooling consumption (kwh/m2_a)", "double"), ("total cooling consumption (twh/a)", "double", "total cooling consumption (twh/a)", "double"), ("verage lighting consumption (kwh/m2_a)", "double", "verage lighting consumption (kwh/m2_a)", "double"), ("total lighting consumption (twh/a)", "double", "total lighting consumption (twh/a)", "double")], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = DataSource0]
Transform0 = ApplyMapping.apply(
    frame = DataSource0, 
    mappings = [
        ("value", "double", "value", "float"),
        ("geometry", "string", "geometry", "string"),
        ("date", "string", "date", "string"),
        ("nuts0", "string", "nuts0", "string"),
        ("nuts1", "string", "nuts1", "string"),
        ("nuts2", "string", "nuts2", "string"),
        ("nuts3", "string", "nuts3", "string"),
        ("measuredelement", "string", "measuredelement", "string"),
        ("unitofmeasure", "string", "unitofmeasure", "string")
    ], 
    transformation_ctx = "Transform0")
## @type: DataSink
## @args: [connection_type = "s3", format = "json", connection_options = {"path": "s3://builthub-inbox-dev/neptune/inbox/", "partitionKeys": []}, transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform0]


# BUILHUB R&D ==================================================================================================================================
#
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dsOwnerID = URIRef(DEFAULT_NS+r"Dataset/29") # Dataset which owns these entities
baseEntityName = "Dataset029/"

# ============================================================================================

#USE THIS FUNCTION TO CREATE UNIQUE ID FOR EACH ENTITY


# ============================================================================================

def getYearMonth(date):

    if (date):
        result = date[:-3]
            
    return result

#ITERATE EACH ROW OF THE JSON

def processPartitionGraph(rowset):
    graph = createBHGraph()

    elementCounter = 0
    for row in rowset:
        # Process dataset's main entity. The entity's ID is created concatenating and hashing all the row's fields.
    
        entID = uuid.uuid4().hex

        dsID = baseEntityName + entID
        rootNode = URIRef(DEFAULT_NS+dsID)
            
        graph.add((rootNode, RDF.type , BUILTHUB.Dataset029))  #CHANGE NAME IF NEEDED
        graph.add((rootNode, DC.identifier , Literal("urn:uuid:"+entID))) # Required for European Commission's entities compatibility
        graph.add((rootNode, SKOS.notation , Literal("urn:uuid:"+entID))) # Required for European Commission's entities compatibility
        # Building relations among BuiltHub's entities.
        graph.add((rootNode, SKOS.broader, dsOwnerID)) # Required for European Commission's entities compatibility
        # Dataset Schema
        graph.add((rootNode, SKOS.inScheme, URIRef(r"http://data.builthub.eu/datasets")))
        #graph.add((rootNode, BUILTHUB.belongsDataset, dsOwnerID))
        
        # DO ALL THE TRANSFORMATIONS THAT YOU WANT
        # HERE YOU WILL ADD ALL THE VALUES TO THE ENTITIES THAT YOU ARE CREATING
        # for example unitMeasure, measuredValues, etc...
        
        #**************************************************COMMON FUNCTIONS:*****************************************
        # country_nuts
        value = row["nuts0"].strip()
        createHasNutsPredicate(graph = graph, rootNode = rootNode, countries = value)
        value = row["nuts1"].strip()
        createHasNutsPredicate(graph = graph, rootNode = rootNode, countries = value)
        value = row["nuts2"].strip()
        createHasNutsPredicate(graph = graph, rootNode = rootNode, countries = value)
        value = row["nuts3"].strip()
        createHasNutsPredicate(graph = graph, rootNode = rootNode, countries = value)

        # geometry (Polygon)
        value=row["geometry"]
        createGeometryPredicate(graph = graph, rootNode = rootNode, geometry = value)

        #unitOfMeasure and value 
        value = row["value"]
        unit = row["unitofmeasure"]
        if value != None and value != 'NULL':
            createMeasurementPredicate(graph = graph, rootNode = rootNode, measurementValue = value, measurementUnit = unit)

        #measuredElement
        value = row["measuredelement"]
        graph.add((rootNode, BUILTHUB.measuredElement, Literal(value, datatype=XSD.string)))

        #timeFrequency
        createPeriodicityPredicate(graph = graph, rootNode = rootNode, periodicity = "monthly")

        #**************************************************OPTIONAL FUNCTIONS:***************************************
        #year
        value = getYearMonth(row["date"])
        lastDay=""
        
        if(value.split("-")[1]=="04" or value.split("-")[1]=="06" or value.split("-")[1]=="09" or value.split("-")[1]=="11"):
            lastDay="30"
        elif(value.split("-")[1]=="02"):
            lastDay="28"
        else:
            lastDay="31"
            
        if(value.split("-")[1]=="02"):
            if(value.split("-")[0]=="2008" or value.split("-")[0]=="2012"):
                lastDay="29"
            else:
                lastDay="28"
                
        initialDate=value+"-01"
        endDate=value+"-"+lastDay
                
        # Verify pre-conditions
        if (type(initialDate) is not str):
            raise TypeError ("Parameter \"initialDate\" must be an string:[" + str(initialDate) + "]")
        if (type(endDate) is not str):
            raise TypeError ("Parameter \"endDate\" must be an string:[" + str(endDate) + "]")

        try:
            datetime.datetime.strptime(initialDate, '%Y-%m-%d')
        except ValueError:
            raise ValueError("Incorrect data format, \"initialDate\" should be YYYY-MM-DD")
        try:
            datetime.datetime.strptime(endDate, '%Y-%m-%d')
        except ValueError:
            raise ValueError("Incorrect data format, \"endDate\" should be YYYY-MM-DD")
            
        if (len(initialDate) < 10 or len(initialDate) > 10):
            raise TypeError ("Invalid period of time:[" + str(initialDate) + "]")
        if (len(endDate) < 10 or len(endDate) > 10):
            raise TypeError ("Invalid period of time:[" + str(endDate) + "]")
                
        createTemporalPredicateFromDates(graph = graph, rootNode = rootNode, initialDate = initialDate, endDate = endDate)
        elementCounter+=1
        # LOOP END
        
    if (elementCounter > 0):
        ttl_name = args['JOB_NAME'] + "-" + uuid.uuid4().hex + ".ttl"
        graph.serialize(destination=ttl_name, format="turtle", base=None, encoding="UTF-8")
            # Upload the TTL file to be ingested by Neptune
        s3 = boto3.client("s3")
        s3TransConf = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10, multipart_chunksize=1024 * 25, use_threads=False)
        s3.upload_file(ttl_name, Bucket="builthub-inbox-dev", Key="neptune/inbox/" + ttl_name, ExtraArgs={"ContentType":"text/turtle"}, Config=s3TransConf)

    del graph

# ============================================================================================

#Transform1 = Transform0.toDF().repartition("nuts") #.coalesce(4)
#Transform1.foreachPartition(processPartitionGraph)
DynamicFrame1 = Transform0.toDF()
DynamicFrame1.foreachPartition(processPartitionGraph)


#DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform0, connection_type = "s3", format = "json", connection_options = {"path": "s3://builthub-inbox-dev/neptune/inbox/", "partitionKeys": []}, transformation_ctx = "DataSink0")
job.commit()


