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

from builthub import DEFAULT_NS, CO, BUILTHUB, createBHGraph, createTemporalPredicate

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
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "builthubgluedb", table_name = "builthub_dataset000", transformation_ctx = "DataSource0")
## @type: ApplyMapping
## @args: [mappings = [("dataset number", "long", "datasetNumber", "long"), ("topic type", "string", "topicType", "string"), ("name", "string", "name", "string"), ("content", "string", "content", "string"), ("author/s", "string", "authors", "string"), ("dataset url", "string", "datasetURL", "string"), ("refernece year", "string", "referenceYears", "string"), ("publication year", "string", "publicationYear", "string"), ("spacial extention", "string", "spacialExtention", "string"), ("granularity", "string", "granularity", "string"), ("methodology url", "string", "methodologyURL", "string"), ("methodology description", "string", "methodologyDescription", "string"), ("accuracy", "string", "accuracy", "string"), ("completeness", "string", "completeness", "string"), ("source", "string", "source", "string"), ("access", "string", "access", "string"), ("license", "string", "license", "string"), ("terms of use", "string", "termsOfUse", "string"), ("availability", "string", "availability", "string"), ("source type", "string", "sourceType", "string")], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = DataSource0]
Transform0 = ApplyMapping.apply(frame = DataSource0, \
    mappings = [("dataset number", "long", "datasetNumber", "long"), \
        ("topic type", "string", "topicType", "string"), \
        ("name", "string", "name", "string"), \
        ("content", "string", "content", "string"), \
        ("author/s", "string", "authors", "string"), \
        ("dataset url", "string", "datasetURL", "string"), \
        ("refernece year", "string", "referenceYears", "string"), \
        ("publication year", "string", "publicationYear", "string"), \
        ("spacial extention", "string", "spacialExtension", "string"), \
        ("granularity", "string", "granularity", "string"), \
        ("methodology url", "string", "methodologyURL", "string"), \
        ("methodology description", "string", "methodologyDescription", "string"), \
        ("accuracy", "string", "accuracy", "string"), \
        ("completeness", "string", "completeness", "string"), \
        ("source", "string", "source", "string"), \
        ("access", "string", "access", "string"), \
        ("license", "string", "license", "string"), \
        ("terms of use", "string", "termsOfUse", "string"), \
        ("availability", "string", "availability", "string"), \
        ("source type", "string", "sourceType", "string")], \
    transformation_ctx = "Transform0")
#
# BUILHUB R&D ==================================================================================================================================
#
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# ============================================================================================

def buildAuthorsBag(authors):
    # Verify pre-conditions
    if (type(authors) is not str):
        return None
        
    authors = authors.replace("\n", "")
    authors = authors.strip()
            
    result = []
    
    if (authors.find(".,") > 0):
        authors = authors.replace(".,", ".|")
    elif (authors.find(",") > 0):
        authors = authors.replace(",", "|")
        
    for item in authors.split("|"):
        result.append(Literal(item.strip(), lang="en"))

    return result

def processPartitionGraph(rowset):
    graph = createBHGraph()

    for row in rowset:
        # Process dataset's NUMBER and create the main entity
        entID = row["datasetNumber"]
        dsID = "Dataset/"+str(entID)
        rootNode = URIRef(DEFAULT_NS+dsID)

        graph.add((rootNode, RDF.type , BUILTHUB.Dataset))
        graph.add((rootNode, DC.identifier , Literal(entID))) # Required for European Commission's entities compatibility
        graph.add((rootNode, SKOS.notation , Literal(entID))) # Required for European Commission's entities compatibility

        # Process dataset's TopicType
        entID = row["topicType"].strip()
        if entID == "A":
            dsTTDes = "Building stock datasets"
        elif entID == "B":
            dsTTDes = "Socio/Economic datasets"
        elif entID == "C":
            dsTTDes = "Climatic datasets"
        else:
            entID = "UNKNOWN"
            dsTTDes = "Unknown topic type"
            
        dsTopicType = "TopicType/" + entID
        graph.add((rootNode, BUILTHUB.topicType , URIRef(DEFAULT_NS+dsTopicType)))
        # Build up a new entity
        graph.add((URIRef(DEFAULT_NS+dsTopicType), RDF.type, BUILTHUB.TopicType))
        graph.add((URIRef(DEFAULT_NS+dsTopicType), DC.identifier , Literal(entID))) # Required for European Commission's entities compatibility
        graph.add((URIRef(DEFAULT_NS+dsTopicType), SKOS.notation , Literal(entID))) # Required for European Commission's entities compatibility
        graph.add((URIRef(DEFAULT_NS+dsTopicType), SKOS.prefLabel, Literal(dsTTDes, lang="en")))
        
        # Process dataset's NAME and CONTENT
        graph.add((rootNode, SKOS.prefLabel , Literal(row["name"].replace("\n","").strip(), lang="en")))
        graph.add((rootNode, DCTERMS.title , Literal(row["name"].replace("\n","").strip(), lang="en")))
        graph.add((rootNode, DCTERMS.description , Literal(row["content"].replace("\n","").strip(), lang="en")))
        
        # Process dataset's AUTHOR/S
        bagSeq = buildAuthorsBag(authors = row["authors"])
        if (bagSeq is not None):
            for item in bagSeq:
                graph.add((rootNode,  DCTERMS.creator, item))
                
        # Process dataset's Source URL
        graph.add((rootNode, BUILTHUB.datasetSource , Literal(row["datasetURL"].strip(), datatype=XSD.anyURY)))

        try:
            # Process dataset's REFERENCE YEARS
            createTemporalPredicate (graph = graph, rootNode = rootNode, timePeriod = row["referenceYears"])
        except Exception as e:
            logger.error (str(e))
           
        # Process dataset's PUBLICATION YEAR
        pubYear = re.sub(r"\D","", row["publicationYear"])
        if (len(pubYear) == 4):
            graph.add((rootNode, DCTERMS.issued , Literal(pubYear + r"-01-01", datatype=XSD.date)))
        
        # Process dataset's SPACIAL EXTENTION
        dsSE = row["spacialExtension"].replace("\n","").strip()
        if (len(dsSE) > 1):
            graph.add((rootNode, BUILTHUB.spacialExtension , Literal(dsSE, datatype=XSD.string)))
        # Process dataset's GRANULARITY
        dsGR = row["granularity"].replace("\n","").strip()
        if len(dsGR) > 1:
            graph.add((rootNode, BUILTHUB.granularity , Literal(dsGR, datatype=XSD.string)))
        # Process dataset's METHODOLOGY URL
        dsMU = re.sub(r"[\s\n]", "", row["methodologyURL"].strip())
        if len(dsMU) > 1:
            graph.add((rootNode, BUILTHUB.methodologySource , Literal(dsMU, datatype=XSD.anyURY)))
        # Process dataset's METHODOLOGY DESCRIPTION
        dsMD = row["methodologyDescription"].replace("\n","").strip()
        if len(dsMD) > 1:
            graph.add((rootNode, BUILTHUB.methodologyDescription , Literal(dsMD, lang="en")))
        # Process dataset's ACCURACY
        dsACC = row["accuracy"].replace("\n","").strip()
        if len(dsACC) > 1:
            graph.add((rootNode, BUILTHUB.accuracy , Literal(dsACC, lang="en")))
        # Process dataset's COMPLETENESS
        dsCMP = row["completeness"].replace("\n","").strip()
        if len(dsCMP) > 1:
            graph.add((rootNode, BUILTHUB.completeness, Literal(dsCMP, lang="en")))
        # Process dataset's SOURCE
        dsSRC = row["source"].replace("\n","").strip()
        if len(dsSRC) > 1:
            graph.add((rootNode, BUILTHUB.source, Literal(dsSRC, lang="en")))
        # Process dataset's ACCESS
        value = row["access"].replace("\n","").strip()
        if len(value) > 1:
            entID = uuid.uuid4().hex
            dsAccess = "Access/" + entID
            graph.add((rootNode, BUILTHUB.access , URIRef(DEFAULT_NS+dsAccess)))
            # Build up a new entity
            graph.add((URIRef(DEFAULT_NS+dsAccess), RDF.type, BUILTHUB.Access))
            graph.add((URIRef(DEFAULT_NS+dsAccess), DC.identifier , Literal(entID))) # Required for European Commission's entities compatibility
            graph.add((URIRef(DEFAULT_NS+dsAccess), SKOS.notation , Literal(entID))) # Required for European Commission's entities compatibility
            graph.add((URIRef(DEFAULT_NS+dsAccess), SKOS.prefLabel, Literal(value, lang="en")))
        # Process dataset's LICENSE
        dsLIC = row["license"].replace("\n","").strip()
        if len(dsLIC) > 1:
            graph.add((rootNode, DCTERMS.license, Literal(dsLIC, lang="en")))
        # Process dataset's TERMS OF USE
        dsTOU = row["termsOfUse"].replace("\n","").strip()
        if len(dsTOU) > 1:
            graph.add((rootNode, BUILTHUB.termsOfUse, Literal(dsTOU, lang="en")))
        # Process dataset's AVAILABILITY
        value = row["availability"].replace("\n","").strip()
        if len(value) > 1:
            entID = uuid.uuid4().hex
            dsAvailability = "Availability/" + entID
            graph.add((rootNode, BUILTHUB.availability , URIRef(DEFAULT_NS+dsAvailability)))
            # Build up a new entity
            graph.add((URIRef(DEFAULT_NS+dsAvailability), RDF.type, BUILTHUB.Availability))
            graph.add((URIRef(DEFAULT_NS+dsAvailability), DC.identifier , Literal(entID))) # Required for European Commission's entities compatibility
            graph.add((URIRef(DEFAULT_NS+dsAvailability), SKOS.notation , Literal(entID))) # Required for European Commission's entities compatibility
            graph.add((URIRef(DEFAULT_NS+dsAvailability), SKOS.prefLabel, Literal(value, lang="en")))
        # Process dataset's SOURCE TYPE
        value = row["sourceType"].replace("\n","").strip()
        if len(value) > 1:
            entID = uuid.uuid4().hex
            dsSourceType = "SourceType/" + entID
            graph.add((rootNode, BUILTHUB.sourceType , URIRef(DEFAULT_NS+dsSourceType)))
            # Build up a new entity
            graph.add((URIRef(DEFAULT_NS+dsSourceType), RDF.type, BUILTHUB.SourceType))
            graph.add((URIRef(DEFAULT_NS+dsSourceType), DC.identifier , Literal(entID))) # Required for European Commission's entities compatibility
            graph.add((URIRef(DEFAULT_NS+dsSourceType), SKOS.notation , Literal(entID))) # Required for European Commission's entities compatibility
            graph.add((URIRef(DEFAULT_NS+dsSourceType), SKOS.prefLabel, Literal(value, lang="en")))

    turtleContent = graph.serialize(format="turtle", base=None, encoding="UTF-8")
    
    #logger.info(turtleContent)
    
    s3 = boto3.client("s3")
    #s3.put_object(Body=turtleContent, Bucket="builthubaws", Key="neptune/inbox/dataset-inventory-" + uuid.uuid4().hex + ".ttl", ContentEncoding="UTF-8", ContentType="text/turtle")
    s3.put_object(Body=turtleContent, Bucket="builthub-inbox-dev", Key="neptune/inbox/dataset-inventory-" + uuid.uuid4().hex + ".ttl", ContentEncoding="UTF-8", ContentType="text/turtle")
    


# ============================================================================================

Transform1 = Transform0.toDF().coalesce(1) # Ensure that only 1 file will be generated
Transform1.foreachPartition(processPartitionGraph)

#
# BUILHUB R&D ==================================================================================================================================
#
job.commit()


