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
import re
#
from rdflib import Graph, Namespace, URIRef, Literal, BNode
from rdflib.namespace import RDF, FOAF, SKOS, RDFS, DC, DCTERMS, XSD, DCAT

from builthub import DEFAULT_NS, DEFAULT_SET_NS, CO, BUILTHUB, EU27, EU28
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
## @args: [database = "builthubgluedb", table_name = "builthub_dataset001", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "builthubgluedb", table_name = "builthub_dataset001", transformation_ctx = "DataSource0")
## @type: ApplyMapping
## @args: [mappings = [("country", "string", "country", "string"), ("country_code", "string", "countryCode", "string"), ("sector", "string", "sector", "string"), ("subsector", "string", "subsector", "string"), ("btype", "string", "btype", "string"), ("bage", "string", "bage", "string"), ("topic", "string", "topic", "string"), ("feature", "string", "feature", "string"), ("type", "string", "type", "string"), ("detail", "string", "detail", "string"), ("estimated", "long", "estimated", "long"), ("value", "double", "value", "double"), ("unit", "string", "unit", "string"), ("source", "string", "source", "string")], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = DataSource0]
Transform0 = ApplyMapping.apply(frame = DataSource0, \
    mappings = [("country", "string", "country", "string"), \
        ("country_code", "string", "countryCode", "string"), \
        ("sector", "string", "sector", "string"), \
        ("subsector", "string", "subsector", "string"), \
        ("btype", "string", "btype", "string"), \
        ("bage", "string", "bage", "string"), \
        ("topic", "string", "topic", "string"), \
        ("feature", "string", "feature", "string"), \
        ("type", "string", "type", "string"), \
        ("detail", "string", "detail", "string"), \
        ("estimated", "long", "estimated", "long"), \
        ("value", "double", "value", "double"), \
        ("unit", "string", "unit", "string"), \
        ("source", "string", "source", "string")], \
    transformation_ctx = "Transform0")
## @type: DataSink
## @args: [connection_type = "s3", format = "json", connection_options = {"path": "s3://builthubaws/neptune/inbox/", "partitionKeys": []}, transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform0]
# DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform0, connection_type = "s3", format = "json", connection_options = {"path": "s3://builthubaws/neptune/inbox/", "partitionKeys": []}, transformation_ctx = "DataSink0")

#
# BUILHUB R&D ==================================================================================================================================
#
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dsOwnerID = URIRef(DEFAULT_NS+r"Dataset/1") # Dataset which owns these entities
baseEntityName = "Dataset001/"

# ============================================================================================
def createBuildingAgePredicate(graph, rootNode, timePeriod):
    # Verify pre-conditions
    if (type(timePeriod) is not str):
        raise TypeError ("Parameter \"timePeriod\" must be an string:[" + str(timePeriod) + "]")
        
    referenceYears = timePeriod.replace("\n", "")
    referenceYears = referenceYears.strip()
    if (len(referenceYears) < 4):
        raise TypeError ("Invalid period of time:[" + str(timePeriod) + "]")
            
    if (re.fullmatch(r"\d{4}", referenceYears)):
        datePeriod = BNode()
        graph.add((rootNode,  BUILTHUB.buildingAge, datePeriod))
        graph.add((datePeriod, RDF.type, DCTERMS.PeriodOfTime))
        graph.add((datePeriod, DCAT.startDate, Literal(referenceYears + r"-01-01", datatype=XSD.date)))
        graph.add((datePeriod, DCAT.endDate, Literal(referenceYears + r"-12-31", datatype=XSD.date)))
        
        return True

    # this is to match a simple year range YYYY,YYYY
    if (re.fullmatch(r"\d{4}\D\d{4}", referenceYears)):
        datePeriod = BNode()
            
        graph.add((rootNode,  BUILTHUB.buildingAge, datePeriod))
        graph.add((datePeriod, RDF.type, DCTERMS.PeriodOfTime))
        graph.add((datePeriod, DCAT.startDate, Literal(referenceYears[0:4] + r"-01-01", datatype=XSD.date)))
        graph.add((datePeriod, DCAT.endDate, Literal(referenceYears[5:9] + r"-12-31", datatype=XSD.date)))

        return True
        
    refYears = referenceYears.replace(" and ", ",")
    refYears = refYears.replace(" ", "")
    if (not re.fullmatch(r"(\d{4}[-/,]*)+", refYears)):
        raise TypeError ("Invalid period of time:[" + str(timePeriod) + "]")
        
    for years in refYears.split(","):
        if (re.fullmatch(r"\d{4}", years)):
            datePeriod = BNode()
            
            graph.add((rootNode,  BUILTHUB.buildingAge, datePeriod))
            graph.add((datePeriod, RDF.type, DCTERMS.PeriodOfTime))
            graph.add((datePeriod, DCAT.startDate, Literal(years + r"-01-01", datatype=XSD.date)))
            graph.add((datePeriod, DCAT.endDate, Literal(years + r"-12-31", datatype=XSD.date)))
        elif (re.fullmatch(r"\d{4}\D\d{4}", years)):
            datePeriod = BNode()
            
            graph.add((rootNode,  BUILTHUB.buildingAge, datePeriod))
            graph.add((datePeriod, RDF.type, DCTERMS.PeriodOfTime))
            graph.add((datePeriod, DCAT.startDate, Literal(years[0:4] + r"-01-01", datatype=XSD.date)))
            graph.add((datePeriod, DCAT.endDate, Literal(years[5:9] + r"-12-31", datatype=XSD.date)))
        else:
            raise TypeError ("Invalid period of time:[" + str(timePeriod) + "]")
            
    return True

# ============================================================================================

def processPartitionGraph(rowset):
    graph = createBHGraph()
    
    for row in rowset:
        # Process dataset's main entity. The entity's ID is created concatenating and hashing all the row's fields.
    
        entID = uuid.uuid4().hex
        dsID = baseEntityName + entID
        rootNode = URIRef(DEFAULT_NS+dsID)

        # ################################################################ #
        # The predicates below this point MUST be included in ALL DATASET ##
        # ################################################################ #
        graph.add((rootNode, RDF.type , BUILTHUB.Dataset001))
        graph.add((rootNode, DC.identifier , Literal("urn:uuid:"+entID))) # Required for European Commission's entities compatibility
        graph.add((rootNode, SKOS.notation , Literal("urn:uuid:"+entID))) # Required for European Commission's entities compatibility
        # Building relations among BuiltHub's entities.
        graph.add((rootNode, SKOS.broader, dsOwnerID)) # Required for European Commission's entities compatibility
        graph.add((rootNode, BUILTHUB.belongsDataset, dsOwnerID))
        # Dataset Schema
        graph.add((rootNode, SKOS.inScheme, URIRef(r"http://data.builthub.eu/datasets")))
        # ################################################################ #
        # The predicates uppon this point MUST be included in ALL DATASET ##
        # ################################################################ #
        
        # Process dataset's COUNTRY / COUNTRY_CODE
        value = row["countryCode"].strip()
        if(value=="gr"):
            value="el"
        createSpatialPredicate (graph = graph, rootNode = rootNode, countries = value)
        #nuts
        createHasNutsPredicate(graph = graph, rootNode = rootNode, countries = value)

        try:
            # Process dataset's BAGE (building age)
            value = row["bage"].strip().replace(" - ", ",")
            if(value == "Before 1945" or value == "Berfore 1945"):
                value = "1900,1944"
            if(value == "Post 2010"):
                value = "2011,2100"
                
            createBuildingAgePredicate (graph = graph, rootNode = rootNode, timePeriod = value)
        except Exception as e:
            logger.error (str(e))

        # year
        value = "2016"
        createTemporalPredicate (graph = graph, rootNode = rootNode, timePeriod = value)

        # Process dataset's SECTOR
        value = row["sector"].strip().title()
        if (len(value) > 0):
            uri = createBHResource (graph = graph, identifier = "Sector/" + value, title = value, description = value, etype = BUILTHUB.Sector)
            graph.add((rootNode, BUILTHUB.sector, uri))

        # Process dataset's SUBSECTOR
        value = row["subsector"].strip().title()
        if (len(value) > 0):
            uri = createBHResource (graph = graph, identifier = "SubSector/" + value, title = value, description = value, etype = BUILTHUB.SubSector)
            graph.add((rootNode, BUILTHUB.subsector, uri))

        # Process dataset's BTYPE
        value = row["btype"].strip().title()
        if (len(value) > 0):
            uri = createBHResource (graph = graph, identifier = "BuildingType/" + value, title = value, description = value, etype = BUILTHUB.BuildingType)
            graph.add((rootNode, BUILTHUB.btype, uri))

        # Process dataset's TOPIC
        value = row["topic"].strip().title()
        if (len(value) > 0):
            uri = createBHResource (graph = graph, identifier = "Topic/" + value, title = value, description = value, etype = BUILTHUB.Topic)
            graph.add((rootNode, BUILTHUB.topic, uri))

        # Process dataset's FEATURE
        value = row["feature"].strip().title()
        if (len(value) > 0):
            uri = createBHResource (graph = graph, identifier = "Feature/" + value, title = value, description = value, etype = BUILTHUB.Feature)
            graph.add((rootNode, BUILTHUB.feature, uri))

        # Process dataset's TOPIC TYPE

        value = row["type"]
        value = re.sub(r"\[.+\]", "", value)
        value = re.sub(r"\(.+\)", "", value)
        value = re.sub(r"[^A-Za-z0-9/\+\- ]", "", value)
        value = value.strip().title()

        #value = row["type"].strip().title()
        #ident = value.replace(" ", "")
        #ident = ident.replace(":", "-")
        #ident = ident.replace(";", "-")
        #ident = re.sub(r"\[.+\]", "", ident)
        #ident = re.sub(r"\(.+\)", "", ident)
        #ident = re.sub(r"[^A-Za-z0-9/\+\-]", "", ident)
        ident = value.replace(" ", "")
        ident = ident.replace(":", "-")
        ident = ident.replace(";", "-")
        if (len(value) > 0):
            uri = createBHResource (graph = graph, identifier = "Type/" + ident, title = value, description = value, etype = BUILTHUB.Type)
            graph.add((rootNode, BUILTHUB.type, uri))

        # Process dataset's DETAIL
        value = row["detail"].strip().title()
        if (len(value) > 0):
            uri = createBHResource (graph = graph, identifier = "Detail/" + value, title = value, description = value, etype = BUILTHUB.Detail)
            graph.add((rootNode, BUILTHUB.detail, uri))

        # Process dataset's ESTIMATED
        value = row["estimated"]
        graph.add((rootNode, BUILTHUB.estimated, Literal(value, datatype=XSD.integer)))

        # Process dataset's SOURCE
        value = row["source"].strip()
        if (len(value) > 0):
            graph.add((rootNode, BUILTHUB.source, Literal(value, lang="en")))

        # Process dataset's VALUE
        msrValue = row["value"]
        msrUnit = row["unit"].strip()
        createMeasurementPredicate(graph = graph, rootNode = rootNode, measurementValue = msrValue, measurementUnit = msrUnit)
        
    turtleContent = graph.serialize(format="turtle", base=None, encoding="UTF-8")
    
    s3 = boto3.client("s3")
    #s3.put_object(Body=turtleContent, Bucket="builthubaws", Key="neptune/inbox/dataset001-" + uuid.uuid4().hex + ".ttl", ContentEncoding="UTF-8", ContentType="text/turtle")
    s3.put_object(Body=turtleContent, Bucket="builthub-inbox-dev", Key="neptune/inbox/dataset001-" + uuid.uuid4().hex + ".ttl", ContentEncoding="UTF-8", ContentType="text/turtle")

# ============================================================================================

Transform1 = Transform0.toDF().coalesce(1) # Ensure that only 1 file will be generated
Transform1.foreachPartition(processPartitionGraph)

#
job.commit()


