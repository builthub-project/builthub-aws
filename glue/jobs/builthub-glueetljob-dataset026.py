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

from builthub import DEFAULT_NS, DEFAULT_SET_NS, CO, ATOLD, BUILTHUB, EU27, EU28, SDMX, SDMX_CODE
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
## @args: [database = "builthubgluedb", table_name = "builthub_dataset026", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "builthubgluedb", table_name = "builthub_dataset026", transformation_ctx = "DataSource0")
## @type: ApplyMapping
## @args: [mappings = [("nuts", "double", "nuts", "double"), ("countries", "string", "countries", "string"), ("years", "string", "years", "string"), ("values", "double", "values", "float"), ("flags1990", "string", "flags1990", "string"), ("flags1991", "string", "flags1991", "string"), ("flags1992", "string", "flags1992", "string"), ("flags1993", "string", "flags1993", "string"), ("flags1994", "string", "flags1994", "string"), ("flags1995", "string", "flags1995", "string"), ("flags1996", "string", "flags1996", "string"), ("flags1997", "string", "flags1997", "string"), ("flags1998", "string", "flags1998", "string"), ("flags1999", "string", "flags1999", "string"), ("flags2000", "string", "flags2000", "string"), ("flags2001", "string", "flags2001", "string"), ("flags2002", "string", "flags2002", "string"), ("flags2003", "string", "flags2003", "string"), ("flags2004", "string", "flags2004", "string"), ("flags2005", "string", "flags2005", "string"), ("flags2006", "string", "flags2006", "string"), ("flags2007", "string", "flags2007", "string"), ("flags2008", "string", "flags2008", "string"), ("flags2009", "string", "flags2009", "string"), ("flags2010", "string", "flags2010", "string"), ("flags2011", "string", "flags2011", "string"), ("flags2012", "string", "flags2012", "string"), ("flags2013", "string", "flags2013", "string"), ("flags2014", "string", "flags2014", "string"), ("flags2015", "string", "flags2015", "string"), ("flags2016", "string", "flags2016", "string"), ("flags2017", "string", "flags2017", "string"), ("flags2018", "string", "flags2018", "string"), ("flags2019", "string", "flags2019", "string"), ("flags2020", "string", "flags2020", "string"), ("sex", "string", "sex", "string")], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = DataSource0]
Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = [("frequency", "string", "frequency", "string"), ("unitOfMeasure", "string", "unitOfMeasure", "string"), ("sex", "string", "sex", "string"), ("age", "string", "age", "string"), ("countries", "string", "countries", "string"), ("years", "int", "years", "string"), ("values", "int", "values", "float"), ("flags", "string", "flags", "string")], transformation_ctx = "Transform0")
## @type: DataSink
## @args: [connection_type = "s3", format = "json", connection_options = {"path": "s3://builthub-inbox-dev/neptune/inbox/", "partitionKeys": []}, transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform0]

# BUILHUB R&D ==================================================================================================================================
#
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dsOwnerID = URIRef(DEFAULT_NS+r"Dataset/26") # Dataset which owns these entities
baseEntityName = "dataset026/"

# ============================================================================================

#USE THIS FUNCTION TO CREATE UNIQUE ID FOR EACH ENTITY

# ============================================================================================

#FUNCTIONS to separate measured element and unit measure

def sexURI(value):
    result = "sex-U"
    if value == "T":
        result = "sex-T"
    elif value == "F":
        result = "sex-F"
    elif value == "M":
        result = "sex-M"
    
    return URIRef(SDMX_CODE + result)


# ============================================================================================
 
#ITERATE EACH ROW OF THE JSON

def processPartitionGraph(rowset):
    graph = createBHGraph()
    
    for row in rowset:
        # Process dataset's main entity. The entity's ID is created concatenating and hashing all the row's fields.
    
        entID = uuid.uuid4().hex

        dsID = baseEntityName + entID
        rootNode = URIRef(DEFAULT_NS+dsID)

        graph.add((rootNode, RDF.type , BUILTHUB.Dataset026))  #CHANGE NAME IF NEEDED
        graph.add((rootNode, DC.identifier , Literal("urn:uuid:"+entID))) # Required for European Commission's entities compatibility
        graph.add((rootNode, SKOS.notation , Literal("urn:uuid:"+entID))) # Required for European Commission's entities compatibility
        # Building relations among BuiltHub's entities.
        graph.add((rootNode, SKOS.broader, dsOwnerID)) # Required for European Commission's entities compatibility
        # Dataset Schema
        graph.add((rootNode, SKOS.inScheme, URIRef(r"http://data.builthub.eu/datasets")))
        #graph.add((rootNode, BUILTHUB.belongsDataset, dsOwnerID))
        
    
        
        # Process dataset's COUNTRY / COUNTRY_CODE
        value = row["countries"]
        if value == "EU27_2007":
            value = value.replace("EU27_2007", "EU27")
        elif value == "EU27_2020":
            value = value.replace("EU27_2020", "EU27")

        try:
            createSpatialPredicate(graph = graph, rootNode = rootNode, countries = value)
            #nuts
            createHasNutsPredicate(graph = graph, rootNode = rootNode, countries = value)
        except BuiltHubException:
            print("Invalid Code")

        # year
        if row["years"] != None:
            value = row["years"].strip().title()
            createTemporalPredicate(graph = graph, rootNode = rootNode, timePeriod = value)

        # value 
        value = row["values"]
        unit = "Number"
        if value != None:
            createMeasurementPredicate(graph = graph, rootNode = rootNode, measurementValue = value, measurementUnit = unit)

        # measuredElement
        value = row["sex"]
        uri = sexURI(value)
        graph.add((rootNode, BUILTHUB.measuredElement, uri))

        # flags
        value = row["flags"]
        if value != 'None':
            graph.add((rootNode, BUILTHUB.availableFlag, Literal(value, datatype=XSD.string)))
        
        
     
    turtleContent = graph.serialize(format="turtle", base=None, encoding="UTF-8")
    
    s3 = boto3.client("s3")
    #change name of dataset if needed
    s3.put_object(Body=turtleContent, Bucket="builthub-inbox-dev", Key="neptune/inbox/dataset026-" + uuid.uuid4().hex + ".ttl", ContentEncoding="UTF-8", ContentType="text/turtle")
    #s3.put_object(Body=turtleContent, Bucket="builthubaws", Key="neptune/inbox/dataset026-" + uuid.uuid4().hex + ".ttl", ContentEncoding="UTF-8", ContentType="text/turtle")

   
        
# ============================================================================================

Transform1 = Transform0.toDF().coalesce(1)
Transform1.foreachPartition(processPartitionGraph)


#DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform0, connection_type = "s3", format = "json", connection_options = {"path": "s3://builthub-inbox-dev/neptune/inbox/", "partitionKeys": []}, transformation_ctx = "DataSink0")
job.commit()


