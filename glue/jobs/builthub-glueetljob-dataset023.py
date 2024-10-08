import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
#
import boto3
import logging
import uuid
#import hashlib
import re
#
from rdflib import Graph, Namespace, URIRef, Literal, BNode
from rdflib.namespace import RDF, FOAF, SKOS, RDFS, DC, DCTERMS, XSD, DCAT

from builthub import BuiltHubDB, DEFAULT_NS, DEFAULT_SET_NS, CO, ATOLD, BUILTHUB, EU27, EU28
from builthub import createBHGraph, createBHResource, createHasNutsPredicate
from builthub import createSpatialPredicate, createTemporalPredicate, createMeasurementPredicate
from builthub import BuiltHubException
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "builthubgluedb", table_name = "builthub_dataset023", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "builthubgluedb", table_name = "builthub_dataset023", transformation_ctx = "DataSource0")
## @type: ApplyMapping
## @args: [mappings = [("GEO", "string", "GEO", "string"), ("TOB", "string", "TOB", "string"), ("OCS", "string", "OCS", "string"), ("POC", "string", "POC", "string"), ("TIME", "int", "TIME", "int"), ("VALUE", "string", "VALUE", "int"), ("FLAGS", "string", "FLAGS", "string"), ("FOOTNOTES", "string", "FOOTNOTES", "string")], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = DataSource0]
Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = [("countries", "string", "countries", "string"), ("typeOfBuilding", "string", "typeOfBuilding", "string"), ("building", "string", "building", "string"), ("years", "string", "years", "string"), ("frequency", "int", "periodOfTime", "int"), ("values", "string", "values", "float"), ("flags", "string", "flags", "string"), ("footnotes", "string", "footnotes", "string")], transformation_ctx = "Transform0")
## @type: DataSink
## @args: [connection_type = "s3", format = "json", connection_options = {"path": "s3://builthub-draftbox-dev/dataset023/", "partitionKeys": []}, transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform0]
# ============================================================================================
# BUILHUB R&D ==================================================================================================================================
#
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dsOwnerID = URIRef(DEFAULT_NS+r"Dataset/23") # Dataset which owns these entities
baseEntityName = "Dataset023/"

# ============================================================================================
#USE THIS FUNCTION TO CREATE UNIQUE ID FOR EACH ENTITY


# ============================================================================================

#FUNCTIONS 

def obtainYears(row):
    text = row
    if row == "Y_LT1919":
        text = text.replace("Y_LT", "1900,")
    elif row == "Y_GE2006":
        text = text.replace("Y_GE", "")
        text = text + ",3000"
    else:
        text = text.replace("Y","").replace("-", ',')
    return text

# ============================================================================================

# ============================================================================================
def processPartitionGraph(rowset):
    graph = createBHGraph()
    
    for row in rowset:
        # Process dataset's main entity. The entity's ID is created concatenating and hashing all the row's fields.
    
        entID = uuid.uuid4().hex

        dsID = baseEntityName + entID
        rootNode = URIRef(DEFAULT_NS+dsID)
            
    

        graph.add((rootNode, RDF.type , BUILTHUB.Dataset023))  #CHANGE NAME IF NEEDED
        graph.add((rootNode, DC.identifier , Literal("urn:uuid:"+entID))) # Required for European Commission's entities compatibility
        graph.add((rootNode, SKOS.notation , Literal("urn:uuid:"+entID))) # Required for European Commission's entities compatibility
        # Building relations among BuiltHub's entities.
        graph.add((rootNode, SKOS.broader, dsOwnerID)) # Required for European Commission's entities compatibility
        # Dataset Schema
        graph.add((rootNode, SKOS.inScheme, URIRef(r"http://data.builthub.eu/datasets")))
        #graph.add((rootNode, BUILTHUB.belongsDataset, dsOwnerID))
        
    
        
        # Process dataset's COUNTRY / COUNTRY_CODE
        value = row["countries"]
        createSpatialPredicate(graph = graph, rootNode = rootNode, countries = value)
        #nuts
        createHasNutsPredicate(graph = graph, rootNode = rootNode, countries = value)
    
        # year
        value = obtainYears(row["years"].strip())
        if value != None:
            if value == 'TOTAL' or value == 'UNK':
                createTemporalPredicate(graph = graph, rootNode = rootNode, timePeriod = '1900,3000')
            else:
                createTemporalPredicate(graph = graph, rootNode = rootNode, timePeriod = value)

        # value 
        value = row["values"]
        if value != None and value != 'NULL' and value != ':':
            value = float(value)
            createMeasurementPredicate(graph = graph, rootNode = rootNode, measurementValue = value, measurementUnit = 'Number')
            
        # type of building 
        value = row["typeOfBuilding"]
        if value == "TOTAL":
            value = value.replace("TOTAL", "Total number of dwellings")
        elif value == "RES":
            value = value.replace("RES", "Conventional dwellings in residential buildings")
        elif value == "NRES":
            value = value.replace("NRES", "Conventional dwellings in non-residential buildings")
        else:
            value = value.replace("UNK","Not stated")
        graph.add((rootNode, BUILTHUB.measuredElement, Literal(value, datatype=XSD.string)))

        # flags and foonotes
        value1 = row["flags"]
        value2 = row["footnotes"]
        if value1 != None and value2 != None:
            value = value1 + ", " + value2
            graph.add((rootNode, BUILTHUB.availableFlag, Literal(value, datatype=XSD.string)))


     
    turtleContent = graph.serialize(format="turtle", base=None, encoding="UTF-8")
    
    s3 = boto3.client("s3")
    #change name of dataset if needed
    s3.put_object(Body=turtleContent, Bucket="builthub-inbox-dev", Key="neptune/inbox/dataset023-" + uuid.uuid4().hex + ".ttl", ContentEncoding="UTF-8", ContentType="text/turtle")
    #s3.put_object(Body=turtleContent, Bucket="builthubaws", Key="neptune/inbox/dataset023-" + uuid.uuid4().hex + ".ttl", ContentEncoding="UTF-8", ContentType="text/turtle")

   
        
# ============================================================================================

Transform1 = Transform0.toDF().coalesce(1)
Transform1.foreachPartition(processPartitionGraph)


#DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform0, connection_type = "s3", format = "json", connection_options = {"path": "s3://builthub-inbox-dev/neptune/inbox/", "partitionKeys": []}, transformation_ctx = "DataSink0")
job.commit()


