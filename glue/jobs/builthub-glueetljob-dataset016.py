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

from builthub import DEFAULT_NS, DEFAULT_SET_NS, CO, ATOLD, BUILTHUB, EU27, EU28, createPeriodicityPredicate, createSiecPredicate
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
## @args: [database = "builthubgluedb", table_name = "builthub_dataset016", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "builthubgluedb", table_name = "builthub_dataset016", transformation_ctx = "DataSource0")
## @type: ApplyMapping
## @args: [mappings = [("country", "string", "country", "string"), ("year", "string", "year", "string"), ("value", "double", "value", "float"), ("Energy balance (Final consumption - other sectors - households - energy use)", "string", "Energy balance (Final consumption - other sectors - households - energy use)", "string"), ("Time frequency", "string", "Time frequency", "string"), ("Standard international energy product classification (SIEC)", "string", "Standard international energy product classification (SIEC)", "string"), ("Unit of measure", "string", "Unit of measure", "string")], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = DataSource0]
Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = [("country", "string", "country", "string"), ("year", "string", "year", "string"), ("value", "double", "value", "float"), ("Energy balance (Final consumption - other sectors - households - energy use)", "string", "energyBalance", "string"), ("Time frequency", "string", "timeFrequency", "string"), ("Standard international energy product classification (SIEC)", "string", "siec", "string"), ("Unit of measure", "string", "unitOfMeasure", "string")], transformation_ctx = "Transform0")
## @type: DataSink
## @args: [connection_type = "s3", format = "json", connection_options = {"path": "s3://builthub-inbox-dev/neptune/inbox/", "partitionKeys": []}, transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform0]

# BUILHUB R&D ==================================================================================================================================
#
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dsOwnerID = URIRef(DEFAULT_NS+r"Dataset/16") # Dataset which owns these entities
baseEntityName = "Dataset016/"

# ============================================================================================

#USE THIS FUNCTION TO CREATE UNIQUE ID FOR EACH ENTITY


# ============================================================================================

#FUNCTIONS 




# ============================================================================================
 
#ITERATE EACH ROW OF THE JSON

def processPartitionGraph(rowset):
    graph = createBHGraph()
    
    for row in rowset:
        # Process dataset's main entity. The entity's ID is created concatenating and hashing all the row's fields.
    
        entID = uuid.uuid4().hex

        dsID = baseEntityName + entID
        rootNode = URIRef(DEFAULT_NS+dsID)
            
    

        graph.add((rootNode, RDF.type , BUILTHUB.Dataset016))  #CHANGE NAME IF NEEDED
        graph.add((rootNode, DC.identifier , Literal("urn:uuid:"+entID))) # Required for European Commission's entities compatibility
        graph.add((rootNode, SKOS.notation , Literal("urn:uuid:"+entID))) # Required for European Commission's entities compatibility
        # Building relations among BuiltHub's entities.
        graph.add((rootNode, SKOS.broader, dsOwnerID)) # Required for European Commission's entities compatibility
        # Dataset Schema
        graph.add((rootNode, SKOS.inScheme, URIRef(r"http://data.builthub.eu/datasets")))
        #graph.add((rootNode, BUILTHUB.belongsDataset, dsOwnerID))
        
    
        
        # Process dataset's COUNTRY / COUNTRY_CODE
        value = row["country"].strip()

        if (value == 'European Union - 27 countries (from 2020)'):
            value = 'EU27'
        if (value == 'European Union - 28 countries (2013-2020)'):
            value = 'EU28'
        if (value == 'Germany (until 1990 former territory of the FRG)'):
            value = 'Germany'

        if value != 'Euro area - 19 countries  (from 2015)' \
        and value != 'Kosovo (under United Nations Security Council Resolution 1244/99)':
            createSpatialPredicate(graph = graph, rootNode = rootNode, countries = value)
            #nuts
            createHasNutsPredicate(graph = graph, rootNode = rootNode, countries = value)

        # year
        value = row["year"].strip().title()
        createTemporalPredicate(graph = graph, rootNode = rootNode, timePeriod = value)

        #measuredElement
        value = row["energyBalance"]
        if value != 'overall' and value != 'other end use':
            graph.add((rootNode, BUILTHUB.measuredElement, Literal("Energy consumption in " + value, datatype=XSD.string)))
        else:
            graph.add((rootNode, BUILTHUB.measuredElement, Literal(value, datatype=XSD.string)))

        # value & unit of measure
        value = row["value"]
        unit = row["unitOfMeasure"]
        if value != None:
            createMeasurementPredicate(graph = graph, rootNode = rootNode, measurementValue = value, measurementUnit = unit)

        #timeFrequency
        createPeriodicityPredicate(graph = graph, rootNode = rootNode, periodicity = "annual")
        
        #siec
        createSiecPredicate(graph = graph, rootNode = rootNode, siec = "Total")
        


    turtleContent = graph.serialize(format="turtle", base=None, encoding="UTF-8")
    
    s3 = boto3.client("s3")
    #change name of dataset if needed
    s3.put_object(Body=turtleContent, Bucket="builthub-inbox-dev", Key="neptune/inbox/dataset016-" + uuid.uuid4().hex + ".ttl", ContentEncoding="UTF-8", ContentType="text/turtle")
    #s3.put_object(Body=turtleContent, Bucket="builthubaws", Key="neptune/inbox/dataset016-" + uuid.uuid4().hex + ".ttl", ContentEncoding="UTF-8", ContentType="text/turtle")
   
        
# ============================================================================================

Transform1 = Transform0.toDF().coalesce(1)
Transform1.foreachPartition(processPartitionGraph)


#DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform0, connection_type = "s3", format = "json", connection_options = {"path": "s3://builthub-inbox-dev/neptune/inbox/", "partitionKeys": []}, transformation_ctx = "DataSink0")
job.commit()


