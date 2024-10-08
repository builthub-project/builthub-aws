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

from builthub import DEFAULT_NS, DEFAULT_SET_NS, CO, BUILTHUB, EU27, EU28 ,ATOLD
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
## @args: [database = "builthubgluedb", table_name = "builthub_dataset017", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "builthubgluedb", table_name = "builthub_dataset017", transformation_ctx = "DataSource0")
## @type: ApplyMapping
## @args: [mappings = [("cities", "string", "cities", "string"), ("values", "double", "values", "float"), ("Entity", "string", "Entity", "string"), ("Units", "string", "units", "string"), ("Measured Values", "string", "Measured Values", "string"), ("MeasurementUnit", "string", "MeasurementUnit", "string")], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = DataSource0]
Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = [("nuts", "string", "nuts", "string"), ("values", "double", "values", "float"), ("Entity", "string", "Entity", "string"), ("Units", "string", "Units", "string"), ("Measured Values", "string", "Measured Values", "string"), ("MeasurementUnit", "string", "MeasurementUnit", "string")], transformation_ctx = "Transform0")
## @type: DataSink
## @args: [connection_type = "s3", format = "json", connection_options = {"path": "s3://builthub-inbox-dev/neptune/inbox/", "partitionKeys": []}, transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform0]

# BUILHUB R&D ==================================================================================================================================
#
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dsOwnerID = URIRef(DEFAULT_NS+r"Dataset/17") # Dataset which owns these entities
baseEntityName = "Dataset017/"

# ============================================================================================

#USE THIS FUNCTION TO CREATE UNIQUE ID FOR EACH ENTITY


# ============================================================================================

#FUNCTIONS 

def obtainYears(row):
    text = row
    if row == "Before 1919":
        text = text.replace("Before ", "1900,")
    elif row == "2006 and later":
        text = text.replace(" and later", ",3000")
    else:
        text = text.replace("  ", ',')
    return text

def formatAttribute(val):
    text = val
    if val == "Before 1919":
        text = text.replace("Before 1919", " built before 1919")
    elif val == "2006 and later":
        text = text.replace("2006 and later", " built after 2006")
    else:
        tx = text.replace("  ", " and ")
        text = " built between " + tx
    return text

# ============================================================================================
 
#ITERATE EACH ROW OF THE JSON

def processPartitionGraph(rowset):
    graph = createBHGraph()
    
    for row in rowset:
        # Process dataset's main entity. The entity's ID is created concatenating and hashing all the row's fields.
    
        entID = uuid.uuid4().hex

        dsID = baseEntityName + entID
        rootNode = URIRef(DEFAULT_NS+dsID)
            
    

        graph.add((rootNode, RDF.type , BUILTHUB.Dataset017))  #CHANGE NAME IF NEEDED
        graph.add((rootNode, DC.identifier , Literal("urn:uuid:"+entID))) # Required for European Commission's entities compatibility
        graph.add((rootNode, SKOS.notation , Literal("urn:uuid:"+entID))) # Required for European Commission's entities compatibility
        # Building relations among BuiltHub's entities.
        graph.add((rootNode, SKOS.broader, dsOwnerID)) # Required for European Commission's entities compatibility
        # Dataset Schema
        graph.add((rootNode, SKOS.inScheme, URIRef(r"http://data.builthub.eu/datasets")))
        #graph.add((rootNode, BUILTHUB.belongsDataset, dsOwnerID))
        
    
        # country and city
        #createSpatialPredicate(graph = graph, rootNode = rootNode, countries = 'Germany')
        nuts = row["nuts"]

        if nuts.startswith("noCode") == False:
            try:
                createSpatialPredicate(graph = graph, rootNode = rootNode, countries = nuts)
                #nuts
                createHasNutsPredicate(graph = graph, rootNode = rootNode, countries = nuts)
            except BuiltHubException:
                print("No Code Available")

        # value
        value = row["values"]
        if value != None:
            createMeasurementPredicate(graph = graph, rootNode = rootNode, measurementValue = value, measurementUnit = 'Number')
     
        # typeOfBuilding
        tob = row["Entity"]
        if tob != None:
            tob = tob.title()
            graph.add((rootNode, BUILTHUB.btype, Literal(tob, datatype=XSD.string)))

        # measuredElement and year
        element = row["Measured Values"] #type of building or year
        #year
        if element == 'year of construction':
            year = row["Units"]
            if year != 'Total':
                value_year = obtainYears(year)
                createTemporalPredicate(graph = graph, rootNode = rootNode, timePeriod = value_year)
        #measuredElement
        value = row["Units"]
        if element == 'year of construction':
            if value != 'Total':
                value = formatAttribute(value)
                graph.add((rootNode, BUILTHUB.measuredElement, Literal(tob + value, datatype=XSD.string)))
        elif element == 'type of building': 
            graph.add((rootNode, BUILTHUB.measuredElement, Literal(value.strip(), datatype=XSD.string)))



    turtleContent = graph.serialize(format="turtle", base=None, encoding="UTF-8")
    
    s3 = boto3.client("s3")
    #change name of dataset if needed
    s3.put_object(Body=turtleContent, Bucket="builthub-inbox-dev", Key="neptune/inbox/dataset017-" + uuid.uuid4().hex + ".ttl", ContentEncoding="UTF-8", ContentType="text/turtle")
    #s3.put_object(Body=turtleContent, Bucket="builthubaws", Key="neptune/inbox/dataset017-" + uuid.uuid4().hex + ".ttl", ContentEncoding="UTF-8", ContentType="text/turtle")

   
        
# ============================================================================================

Transform1 = Transform0.toDF().coalesce(1)
Transform1.foreachPartition(processPartitionGraph)


#DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform0, connection_type = "s3", format = "json", connection_options = {"path": "s3://builthub-inbox-dev/neptune/inbox/", "partitionKeys": []}, transformation_ctx = "DataSink0")
job.commit()


