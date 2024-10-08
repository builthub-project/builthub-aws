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

from builthub import DEFAULT_NS, DEFAULT_SET_NS, CO, ATOLD, BUILTHUB, EU27, EU28
from builthub import createBHGraph, createBHResource, createHasNutsPredicate
from builthub import createSpatialPredicate, createTemporalPredicate, createMeasurementPredicate, createSiecPredicate
from builthub import BuiltHubException

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "builthubgluedb", table_name = "builthub_dataset024", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "builthubgluedb", table_name = "builthub_dataset024", transformation_ctx = "DataSource0")
## @type: ApplyMapping
## @args: [mappings = [("Country", "string", "country", "string"), ("measuredElement", "string", "measuredElement", "string"), ("values", "string", "values", "float"), ("energies", "string", "energies", "string")], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = DataSource0]
Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = [("Country", "string", "country", "string"), ("measuredElement", "string", "measuredElement", "string"), ("values", "string", "values", "float"), ("energies", "string", "energies", "string")], transformation_ctx = "Transform0")
## @type: DataSink
## @args: [connection_type = "s3", format = "json", connection_options = {"path": "s3://builthub-inbox-dev/neptune/inbox/", "partitionKeys": []}, transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform0]

# BUILHUB R&D ==================================================================================================================================
#
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dsOwnerID = URIRef(DEFAULT_NS+r"Dataset/24") # Dataset which owns these entities
baseEntityName = "Dataset024/"

# ============================================================================================

#USE THIS FUNCTION TO CREATE UNIQUE ID FOR EACH ENTITY


# ============================================================================================

#FUNCTIONS to separate measured element and unit measure

def obtainMeasuredElement(row):
    text = re.sub(r'\(([^)]+\))', "", row)
    return text

def obtainUnitMeasure(row):
    return re.search('\(([^)]+)', row).group(1)

def format(row):
    text = row
    if row == "price ":
        text = text.replace("price ", "Average price")
    return text.strip().title()

# ============================================================================================
 
#ITERATE EACH ROW OF THE JSON

def processPartitionGraph(rowset):
    graph = createBHGraph()
    
    for row in rowset:
        # Process dataset's main entity. The entity's ID is created concatenating and hashing all the row's fields.
    
        entID = uuid.uuid4().hex

        dsID = baseEntityName + entID
        rootNode = URIRef(DEFAULT_NS+dsID)
            
    

        graph.add((rootNode, RDF.type , BUILTHUB.Dataset024))  #CHANGE NAME IF NEEDED
        graph.add((rootNode, DC.identifier , Literal("urn:uuid:"+entID))) # Required for European Commission's entities compatibility
        graph.add((rootNode, SKOS.notation , Literal("urn:uuid:"+entID))) # Required for European Commission's entities compatibility
        # Building relations among BuiltHub's entities.
        graph.add((rootNode, SKOS.broader, dsOwnerID)) # Required for European Commission's entities compatibility
        # Dataset Schema
        graph.add((rootNode, SKOS.inScheme, URIRef(r"http://data.builthub.eu/datasets")))
        #graph.add((rootNode, BUILTHUB.belongsDataset, dsOwnerID))
        
    
        
        # Process dataset's COUNTRY / COUNTRY_CODE
        value = row["country"].strip()

        if value == 'UnitedKingdom':
            value = value.replace("UnitedKingdom", "united kingdom")
        elif value == "NorthMacedonia":
            value = value.replace("NorthMacedonia", "north macedonia")
        elif value == "Bosnia&Herzegovina":
            value = value.replace("Bosnia&Herzegovina", "bosnia and herzegovina")

        if value != 'Euroarea' and value != 'Kosovo':
            createSpatialPredicate(graph = graph, rootNode = rootNode, countries = value)
            #nuts
            createHasNutsPredicate(graph = graph, rootNode = rootNode, countries = value)

        # year
        createTemporalPredicate(graph = graph, rootNode = rootNode, timePeriod = '2019')

        # measured element#values
        value = row["measuredElement"]
        value = obtainMeasuredElement(value)
        value = format(value)
        graph.add((rootNode, BUILTHUB.measuredElement, Literal(value, datatype=XSD.string)))

        #unit measure and value 
        value2 = row["measuredElement"]
        value2 = obtainUnitMeasure(value2)
        value3 = row["values"]
        if value3 != None:
            createMeasurementPredicate(graph = graph, rootNode = rootNode, measurementValue = value3, measurementUnit = value2)

        #energies
        value = row["energies"].title()
        createSiecPredicate(graph = graph, rootNode = rootNode, siec = value)
   
     

    turtleContent = graph.serialize(format="turtle", base=None, encoding="UTF-8")
    
    s3 = boto3.client("s3")
    #change name of dataset if needed
    s3.put_object(Body=turtleContent, Bucket="builthub-inbox-dev", Key="neptune/inbox/dataset024-" + uuid.uuid4().hex + ".ttl", ContentEncoding="UTF-8", ContentType="text/turtle")
    #s3.put_object(Body=turtleContent, Bucket="builthubaws", Key="neptune/inbox/dataset024-" + uuid.uuid4().hex + ".ttl", ContentEncoding="UTF-8", ContentType="text/turtle")
   
        
# ============================================================================================

Transform1 = Transform0.toDF().coalesce(1)
Transform1.foreachPartition(processPartitionGraph)


#DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform0, connection_type = "s3", format = "json", connection_options = {"path": "s3://builthub-inbox-dev/neptune/inbox/", "partitionKeys": []}, transformation_ctx = "DataSink0")
job.commit()


