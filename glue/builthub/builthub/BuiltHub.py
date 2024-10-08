import sys
import logging
import re
import json
import urllib3
import boto3
import datetime
#
from urllib.parse import unquote_plus, urlencode
#
from rdflib import Graph, Namespace, URIRef, Literal, BNode
from rdflib.namespace import RDF, FOAF, SKOS, RDFS, DC, DCTERMS, XSD, DCAT

#
# BUILHUB R&D ==================================================================================================================================
#
DEFAULT_NS = Namespace(r"http://data.builthub.eu/resource/")
DEFAULT_SET_NS = Namespace(DEFAULT_NS + r"set/")
#

NUTSOLD = Namespace(r"http://data.europa.eu/nuts/code/")

ATOLD = Namespace(r"http://publications.europa.eu/resource/authority/country/") # European Commission's Open Data standard prefix for Country's entities
#
CO = Namespace(r"http://purl.org/co/") # To work with Collections
QUDT = Namespace(r"http://qudt.org/1.1/schema/qudt#")
QUDTU = Namespace(r"http://qudt.org/1.1/vocab/unit#")  
#
CDT = Namespace(r"http://w3id.org/lindt/custom_datatypes#")
#
ATOLD = Namespace(r"http://publications.europa.eu/resource/authority/country/") # European Commission's Open Data standard prefix for Country's entities
GN = Namespace("http://www.geonames.org/ontology#")
WGS84_POS = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
#
BUILTHUB = Namespace(r"http://data.builthub.eu/ontology/cbhsv#")
#
FREQ = Namespace(r"http://purl.org/cld/freq/") #to work with frequencies
SIEC = Namespace(r"http://dd.eionet.europa.eu/vocabulary/eurostat/siec/") #siec 
SDMX = Namespace(r"http://purl.org/linked-data/sdmx#") #sdmx 
SDMX_CODE = Namespace(r"http://purl.org/linked-data/sdmx/2009/code#") #sdmx-code 
#
GEO = Namespace(r"http://www.opengis.net/ont/geosparql#") #to work with geometries
# Exceptions
#
class BuiltHubException(Exception):
    pass

#
# Utility functions and data
#

UK = NUTSOLD + "UK"

EU27 = [
    NUTSOLD + "AT",
    NUTSOLD + "BE",
    NUTSOLD + "BG",
    NUTSOLD + "HR",
    NUTSOLD + "CY",
    NUTSOLD + "CZ",
    NUTSOLD + "DK",
    NUTSOLD + "EE",
    NUTSOLD + "FI",
    NUTSOLD + "FR",
    NUTSOLD + "DE",
    NUTSOLD + "EL",
    NUTSOLD + "HU",
    NUTSOLD + "IE",
    NUTSOLD + "IT",
    NUTSOLD + "LV",
    NUTSOLD + "LT",
    NUTSOLD + "LU",
    NUTSOLD + "MT",
    NUTSOLD + "NL",
    NUTSOLD + "PL",
    NUTSOLD + "PT",
    NUTSOLD + "RO",
    NUTSOLD + "SK",
    NUTSOLD + "SI",
    NUTSOLD + "ES",
    NUTSOLD + "SE"]
        
EU28 = [
    NUTSOLD + "AT",
    NUTSOLD + "BE",
    NUTSOLD + "BG",
    NUTSOLD + "HR",
    NUTSOLD + "CY",
    NUTSOLD + "CZ",
    NUTSOLD + "DK",
    NUTSOLD + "EE",
    NUTSOLD + "FI",
    NUTSOLD + "FR",
    NUTSOLD + "DE",
    NUTSOLD + "EL",
    NUTSOLD + "HU",
    NUTSOLD + "IE",
    NUTSOLD + "IT",
    NUTSOLD + "LV",
    NUTSOLD + "LT",
    NUTSOLD + "LU",
    NUTSOLD + "MT",
    NUTSOLD + "NL",
    NUTSOLD + "PL",
    NUTSOLD + "PT",
    NUTSOLD + "RO",
    NUTSOLD + "SK",
    NUTSOLD + "SI",
    NUTSOLD + "ES",
    NUTSOLD + "SE",
    UK]

EU27_PUB = [
    ATOLD + "AT",
    ATOLD + "BE",
    ATOLD + "BG",
    ATOLD + "HR",
    ATOLD + "CY",
    ATOLD + "CZ",
    ATOLD + "DK",
    ATOLD + "EE",
    ATOLD + "FI",
    ATOLD + "FR",
    ATOLD + "DE",
    ATOLD + "EL",
    ATOLD + "HU",
    ATOLD + "IE",
    ATOLD + "IT",
    ATOLD + "LV",
    ATOLD + "LT",
    ATOLD + "LU",
    ATOLD + "MT",
    ATOLD + "NL",
    ATOLD + "PL",
    ATOLD + "PT",
    ATOLD + "RO",
    ATOLD + "SK",
    ATOLD + "SI",
    ATOLD + "ES",
    ATOLD + "SE"]
        
        
EU28_PUB = [
    ATOLD + "AT",
    ATOLD + "BE",
    ATOLD + "BG",
    ATOLD + "HR",
    ATOLD + "CY",
    ATOLD + "CZ",
    ATOLD + "DK",
    ATOLD + "EE",
    ATOLD + "FI",
    ATOLD + "FR",
    ATOLD + "DE",
    ATOLD + "EL",
    ATOLD + "HU",
    ATOLD + "IE",
    ATOLD + "IT",
    ATOLD + "LV",
    ATOLD + "LT",
    ATOLD + "LU",
    ATOLD + "MT",
    ATOLD + "NL",
    ATOLD + "PL",
    ATOLD + "PT",
    ATOLD + "RO",
    ATOLD + "SK",
    ATOLD + "SI",
    ATOLD + "ES",
    ATOLD + "SE",
    UK]

def createBHGraph ():
    graph = Graph()

    graph.bind("rdf", RDF)
    graph.bind("rdfs", RDFS)
    graph.bind("foaf", FOAF)
    graph.bind("dc", DC)
    graph.bind("dcterms", DCTERMS)
    graph.bind("xsd", XSD)
    graph.bind("skos", SKOS)
    graph.bind("dcat", DCAT)
    graph.bind("atold", ATOLD)
    graph.bind("nuts", NUTSOLD)
    graph.bind("co", CO)
    graph.bind("qudt", QUDT)
    graph.bind("qudtu", QUDTU)
    graph.bind("cdt", CDT)
    graph.bind("gn", GN)
    graph.bind("wgs84_pos", WGS84_POS)
    #
    graph.bind("cbhsv", BUILTHUB)
    graph.bind("siec", SIEC) 
    graph.bind("geo", GEO) 
    graph.bind("sdmx", SDMX) 
    graph.bind("sdmx-code", SDMX_CODE) 


    return graph

s3 = boto3.client('s3')
response = s3.get_object(Bucket='aws-glue-assets-613158767627-eu-central-1', Key='scripts/data/_publicationsIRIs.json')
content = response['Body'].read().decode('utf-8')
publications = json.loads(content)

def _findCountryIRI (alpha2):
    if type(alpha2) is not str:
        raise TypeError ("Parameter \"alpha2\" must be a string:[" + str(alpha2) + "]")
    if len(alpha2) != 2:
        raise TypeError ("Parameter \"alpha2\" must be an ISO3166-Alpha2 code:[" + alpha2 + "]")

    if (alpha2 in publications) :
        return publications[alpha2]
    else:
        #raise BuiltHubException ("ISO3166-Alpha2 code [" + alpha2 + "] not found")
        return None

    #return NUTSOLD + "code/" + alpha2.upper()

def _findNutsIRI (nutscode):
    if type(nutscode) is not str:
        raise TypeError ("Parameter \"nutscode\" must be a string:[" + str(nutscode) + "]")
        
    '''if len(nutscode) > 5: #then we are not in nuts3 anymore
        raise TypeError ("Parameter \"nutscode\" is not valid:[" + str(nutscode) + "]")'''

    return NUTSOLD + nutscode.upper()

s3 = boto3.client('s3')
response = s3.get_object(Bucket='aws-glue-assets-613158767627-eu-central-1', Key='scripts/data/_countryNamesToISO3166Alpha2.json')
content = response['Body'].read().decode('utf-8')
countriesalpha2 = json.loads(content)

def findIRIByCountryName (countryName):
    result = None
    key = countryName.strip().lower()

    if (key in countriesalpha2) :
        alpha2 = countriesalpha2[key]
        result = _findCountryIRI (alpha2 = alpha2)
        if (result is not None) :
            return result
        
    return None

s3 = boto3.client('s3')
response = s3.get_object(Bucket='aws-glue-assets-613158767627-eu-central-1', Key='scripts/data/_iso3166Alpha3toAlpha2.json')
content = response['Body'].read().decode('utf-8')
alpha3alpha2 = json.loads(content)

def findIRIByCountryCode (countryCode):
    key = countryCode.strip().lower()
    
    if (key == "eu27" or key == "eu28"):
        return None
    
    if (len(key) == 2):
        return  _findCountryIRI (alpha2 = key)

    elif (len(key) > 2):
        if (not key[2].isdigit()):

            if key.lower() in alpha3alpha2:
                uri =_findCountryIRI (alpha2 = alpha3alpha2[key.lower()])
                return uri
            elif (key.lower() not in countriesalpha2):
                return _findCountryIRI(alpha2 = key[:2])
        elif (key[2].isdigit() ):
            return _findCountryIRI(alpha2 = key[:2])
    
    return None


    '''
    elif (len(key) == 3 and not key[2].isdigit() and not (key[2] == 'z' or key[2] == 's')):

        uri =_findCountryIRI (alpha2 = _iso3166Alpha3toAlpha2[key.lower()])

        return uri

        #if uri is None:
        #    if (key in _countryNamesToISO3166Alpha2) :
        #        alpha2 = _countryNamesToISO3166Alpha2[key]
        #        return  _findCountryIRI (alpha2 = alpha2)
        #else:
        #    return uri

    elif (len(key) > 2):
        #to retrieve the country publications from a nuts code > 2 (i.e. BE1, IT11)
        if (key[2].isdigit()):
            return _findCountryIRI(alpha2 = key[:2])
        if ((key[2] == 'z' or key[2] == 's') and len(key) < 6):
            return _findCountryIRI(alpha2 = key[:2])

    return None
    '''

def createBHResource(graph, identifier, title, description = None, etype = SKOS.Concept):
    #Preconditions
    if type(graph) is not Graph:
        raise TypeError ("Parameter \"graph\" must be a RDFLIB Graph()")
    if type(identifier) is not str:
        raise TypeError ("Parameter \"identifier\" must be an string:[" + str(identifier) + "]")
    if type(title) is not str:
        raise TypeError ("Parameter \"title\" must be an string:[" + str(title) + "]")

    key = identifier.strip().replace(" ", "")
    uri = URIRef(DEFAULT_SET_NS + key)

    graph.add((uri, RDF.type, etype))
    graph.add((uri, DC.identifier , Literal(key)))
    graph.add((uri, SKOS.notation , Literal(key)))
    graph.add((uri, SKOS.prefLabel, Literal(title, lang="en")))
    graph.add((uri, DCTERMS.title, Literal(title, lang="en")))
    if (description is not None):
        graph.add((uri, DCTERMS.description, Literal(description, lang="en")))
    
    return uri

def createTemporalPredicate(graph, rootNode, timePeriod):
    # Verify pre-conditions
    if (type(timePeriod) is not str):
        raise TypeError ("Parameter \"timePeriod\" must be an string:[" + str(timePeriod) + "]")
        
    referenceYears = timePeriod.replace("\n", "")
    referenceYears = referenceYears.strip()
    if (len(referenceYears) < 4):
        raise TypeError ("Invalid period of time:[" + str(timePeriod) + "]")
            
    if (re.fullmatch(r"\d{4}", referenceYears)):
        datePeriod = BNode()
        graph.add((rootNode,  DCTERMS.temporal, datePeriod))
        graph.add((datePeriod, RDF.type, DCTERMS.PeriodOfTime))
        graph.add((datePeriod, DCAT.startDate, Literal(referenceYears + r"-01-01", datatype=XSD.date)))
        graph.add((datePeriod, DCAT.endDate, Literal(referenceYears + r"-12-31", datatype=XSD.date)))
        
        return True

    # this is to match a simple year range YYYY,YYYY
    if (re.fullmatch(r"\d{4}\D\d{4}", referenceYears)):
        datePeriod = BNode()
            
        graph.add((rootNode,  DCTERMS.temporal, datePeriod))
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
            
            graph.add((rootNode,  DCTERMS.temporal, datePeriod))
            graph.add((datePeriod, RDF.type, DCTERMS.PeriodOfTime))
            graph.add((datePeriod, DCAT.startDate, Literal(years + r"-01-01", datatype=XSD.date)))
            graph.add((datePeriod, DCAT.endDate, Literal(years + r"-12-31", datatype=XSD.date)))
        elif (re.fullmatch(r"\d{4}\D\d{4}", years)):
            datePeriod = BNode()
            
            graph.add((rootNode,  DCTERMS.temporal, datePeriod))
            graph.add((datePeriod, RDF.type, DCTERMS.PeriodOfTime))
            graph.add((datePeriod, DCAT.startDate, Literal(years[0:4] + r"-01-01", datatype=XSD.date)))
            graph.add((datePeriod, DCAT.endDate, Literal(years[5:9] + r"-12-31", datatype=XSD.date)))
        else:
            raise TypeError ("Invalid period of time:[" + str(timePeriod) + "]")
            
    return True

def createTemporalPredicateFromDates(graph, rootNode, initialDate, endDate):
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
            
    datePeriod = BNode()
    graph.add((rootNode,  DCTERMS.temporal, datePeriod))
    graph.add((datePeriod, RDF.type, DCTERMS.PeriodOfTime))
    graph.add((datePeriod, DCAT.startDate, Literal(initialDate, datatype=XSD.date)))
    graph.add((datePeriod, DCAT.endDate, Literal(endDate, datatype=XSD.date)))
        
    return True

def createGeometryPredicate(graph, rootNode, geometry):
    # Verify pre-conditions
    if (type(geometry) is not str):
        raise TypeError ("Parameter \"initialDate\" must be an string:[" + str(geometry) + "]")

    spatial = BNode()
    geonode = BNode()
    graph.add((rootNode, DCAT.spatial, spatial))
    graph.add((spatial,  GEO.hasGeometry, geonode))
    graph.add((geonode, GEO.asWKT, Literal(geometry, datatype=XSD.string)))
        
    return True

def createCountrySet(graph, rootNode, countries):
    #Preconditions
    if type(countries) is not str:
        raise TypeError ("Parameter \"countries\" must be an string:[" + str(countries) + "]")
    if type(graph) is not Graph:
        raise TypeError ("Parameter \"graph\" must be a RDFLIB Graph()")

    key = countries.strip().upper().replace(" ", "")
    uriSet = BNode()

    if ((key == r"EU28") or (key == r"EU27+UK")):
        graph.add((rootNode,  DCAT.spatial, uriSet))

        graph.add((uriSet, RDF.type, CO.Set))
        graph.add((uriSet, DC.identifier , Literal(key)))
        graph.add((uriSet, SKOS.prefLabel , Literal(key, lang="en")))
        graph.add((uriSet, SKOS.notation , Literal(key)))
        for uri in EU28_PUB:
            graph.add((uriSet, CO.element, URIRef(uri)))
    elif (key == r"EU27"):
        graph.add((rootNode,  DCAT.spatial, uriSet))

        graph.add((uriSet, RDF.type, CO.Set))
        graph.add((uriSet, DC.identifier , Literal(key)))
        graph.add((uriSet, SKOS.prefLabel , Literal(key, lang="en")))
        graph.add((uriSet, SKOS.notation , Literal(key)))
        for uri in EU27_PUB:
            graph.add((uriSet, CO.element, URIRef(uri)))
    else:
        raise BuiltHubException ("Unknown country set: [" + countries + "]")
    
    return True

s3 = boto3.client('s3')
response = s3.get_object(Bucket='aws-glue-assets-613158767627-eu-central-1', Key='scripts/data/_nutsIRIs.json')
content = response['Body'].read().decode('utf-8')
nuts_iris = json.loads(content)

def findNutsIRIByCountryCode (countryCode):
    key = countryCode.strip().lower()
    
    if (key in nuts_iris):
        return  _findNutsIRI (nutscode = key)

    elif (len(key) > 2):
        #nuts code with 3 or more characters
        if (len(key) == 3):
            return _findNutsIRI(nutscode = key)
        if (len(key) > 3 and (key[2].isdigit() or key[3].isdigit()) and len(key) < 6):
            return _findNutsIRI(nutscode = key)
        if (len(key) > 3 and (key[3] == 'z' or key[3] == 's') and len(key) < 6):
            return _findNutsIRI(nutscode = key)
        if (len(key) > 6 and key[5]=="_"):
            return _findNutsIRI(nutscode = key)

    return None

def createMultipleCountryPredicate(graph, rootNode, countries):
    #Preconditions
    if type(countries) is not str:
        raise TypeError ("Parameter \"countries\" must be an string:[" + str(countries) + "]")
    if type(graph) is not Graph:
        raise TypeError ("Parameter \"graph\" must be a RDFLIB Graph()")

    key = countries.strip().upper().replace(" ", "")

    if (key == 'EU27'):
        for country in EU27:
            countryCode = country.split("code/", 1)[1]
            uri = findIRIByCountryCode(countryCode = countryCode)
            uri_nuts = findNutsIRIByCountryCode(countryCode)
            if uri is not None:
                graph.add((rootNode, DCAT.spatial, URIRef(uri)))
                graph.add((rootNode, BUILTHUB.hasNUTS, URIRef(uri_nuts)))


    elif (key == 'EU28'):
        for country in EU28:
            countryCode = country.split("code/", 1)[1]
            uri = findIRIByCountryCode(countryCode = countryCode)
            uri_nuts = findNutsIRIByCountryCode(countryCode)
            if uri is not None:
                graph.add((rootNode, DCAT.spatial, URIRef(uri)))
                graph.add((rootNode, BUILTHUB.hasNUTS, URIRef(uri_nuts)))

    return True
    
def createSpatialPredicate(graph, rootNode, countries):
    uri = findIRIByCountryCode(countryCode = countries)
    if uri is None:
        uri = findIRIByCountryName(countryName = countries)
        if uri is None:
            #createMultipleCountryPredicate(graph = graph, rootNode = rootNode, countries = countries)
            createCountrySet(graph, rootNode, countries)
        else:
            graph.add((rootNode, DCAT.spatial, URIRef(uri)))
    else:
        graph.add((rootNode, DCAT.spatial, URIRef(uri)))



def findNutsIRIByCountryName(countryName):
    result = None
    key = countryName.strip().lower()

    if (key in countriesalpha2) :
        alpha2 = countriesalpha2[key]
        if alpha2 in nuts_iris:
            result = nuts_iris[alpha2]
            if (result is not None) :
                return result
        
    return None

def createNutsSet(graph, rootNode, countries):
     #Preconditions
    if type(countries) is not str:
        raise TypeError ("Parameter \"countries\" must be an string:[" + str(countries) + "]")
    if type(graph) is not Graph:
        raise TypeError ("Parameter \"graph\" must be a RDFLIB Graph()")

    key = countries.strip().upper().replace(" ", "")
    uriSetNuts = BNode()

    if ((key == r"EU28") or (key == r"EU27+UK")):
        graph.add((rootNode,  BUILTHUB.hasNUTS, uriSetNuts))

        graph.add((uriSetNuts, RDF.type, CO.Set))
        graph.add((uriSetNuts, DC.identifier , Literal(key)))
        graph.add((uriSetNuts, SKOS.prefLabel , Literal(key, lang="en")))
        graph.add((uriSetNuts, SKOS.notation , Literal(key)))
        for uri in EU28:
            graph.add((uriSetNuts, CO.element, URIRef(uri)))
    elif (key == r"EU27"):
        graph.add((rootNode, BUILTHUB.hasNUTS, uriSetNuts))

        graph.add((uriSetNuts, RDF.type, CO.Set))
        graph.add((uriSetNuts, DC.identifier , Literal(key)))
        graph.add((uriSetNuts, SKOS.prefLabel , Literal(key, lang="en")))
        graph.add((uriSetNuts, SKOS.notation , Literal(key)))
        for uri in EU27:
            graph.add((uriSetNuts, CO.element, URIRef(uri)))
    else:
        raise BuiltHubException ("Unknown country set: [" + countries + "]")
    
    return True

def countryFromEU(countryName):
    key = countryName.strip().lower()
    if (key in countriesalpha2) :
        alpha2 = countriesalpha2[key]
        if alpha2 in nuts_iris:
            return True
    else:    
        return False

def createHasNutsPredicate(graph, rootNode, countries):
    if (countries == "EU27" or countries == "EU28"):
        return createNutsSet(graph, rootNode, countries)

    uri = findNutsIRIByCountryCode(countryCode = countries)
    if uri is None:
        uri = findNutsIRIByCountryName(countryName = countries)
        if uri is not None:
            graph.add((rootNode, BUILTHUB.hasNUTS, URIRef(uri)))
        
        else:
            # countries inside EU but unknown nuts
            if countryFromEU(countries):
                graph.add((rootNode, BUILTHUB.hasNUTS, URIRef(r"http://data.builthub.eu/resource/nuts/NKN")))
            # for countries outside the EU (not applicable)
            else:
                graph.add((rootNode, BUILTHUB.hasNUTS, URIRef(r"http://data.builthub.eu/resource/nuts/NAP")))

    else:
        graph.add((rootNode, BUILTHUB.hasNUTS, URIRef(uri)))    

    return True

def _normalizeMeasurementUnit(value):  
    #value = value.replace(" ","")
    value = value.replace("_","")
    
    #"MmÂ²"
    if value == "Mm²":
        return "Mm2"
        
    #"kWh/mÂ²/year"
    if value == "kWh/m²/year":
        return "kWh/m2/yr"

    if value == "kWh/m2a":
        return value.replace("a", "/yr")
        
    if value == "TWh/a":
        return value.replace("a", "yr")
        
    if value == "TWh/year":
        return value.replace("year", "yr")

    if value == "Percentage":
        return "%"
    
    if value == "National currency" or value == "National currency (NC)":
        return "NC"
    
    if value == "Purchasing power standard (PPS)" or value == "Purchasing power standard" :
        return "PPS"
    
    if value == "Thousand tonnes of oil equivalent (TOE)" or value == "Thousand tonnes of oil equivalent":
        return "TOE"
 
    return value.replace(" ","")

s3 = boto3.client('s3')

responseQUDT = s3.get_object(Bucket='aws-glue-assets-613158767627-eu-central-1', Key='scripts/data/_qudtDictionary.json')
contentQUDT = responseQUDT['Body'].read().decode('utf-8')
dictQUDT = json.loads(contentQUDT)

responseUCUM = s3.get_object(Bucket='aws-glue-assets-613158767627-eu-central-1', Key='scripts/data/_ucumDictionary.json')
contentUCUM = responseUCUM['Body'].read().decode('utf-8')
dictUCUM = json.loads(contentUCUM)

def createMeasurementPredicate(graph, rootNode, measurementValue, measurementUnit):
    # Verify pre-conditions
    if type(graph) is not Graph:
        raise TypeError ("Parameter \"graph\" must be a RDFLIB Graph()")
    if type(rootNode) is not URIRef:
        raise TypeError ("Parameter \"rootNode\" must be a RDFLIB URIRef()")
    if (type(measurementValue) is not float):
        raise TypeError ("Parameter \"measurementValue\" must be a float:[" + str(measurementValue) + "]")
    if (type(measurementUnit) is not str):
        raise TypeError ("Parameter \"measurementUnit\" must be a string:[" + str(measurementUnit) + "]")

    if (len(measurementUnit) < 1):
        raise TypeError ("Parameter \"measurementUnit\" must have content:[" + measurementUnit + "]")

    measurementUnit = _normalizeMeasurementUnit(measurementUnit)
    measurementUnit.strip()

    # Process dataset's UNIT and VALUE
    graph.add((rootNode, BUILTHUB.measurementUnit, Literal(measurementUnit, datatype=XSD.string)))
    graph.add((rootNode, BUILTHUB.measurementValue, Literal(measurementValue, datatype=XSD.float)))

    unitIRI = ""
    if (measurementUnit in dictQUDT):
        unitIRI = dictQUDT[measurementUnit]

    # Process QUDT ontology
    qudtNode = BNode()
    graph.add((rootNode,  BUILTHUB.measurementQUDT, qudtNode))
    graph.add((qudtNode, RDF.type, QUDT.QuantityValue))
    graph.add((qudtNode, QUDT.unit, URIRef(unitIRI)))
    graph.add((qudtNode, QUDT.numericValue, Literal(measurementValue, datatype=XSD.float)))

    unitIRI = ""
    if (measurementUnit in dictUCUM):
        unitIRI = dictUCUM[measurementUnit]

    # Process UCUM ontology
    value = str(measurementValue) + " " + unitIRI
    graph.add((rootNode, BUILTHUB.measurementUCUM, Literal(value, datatype=CDT.ucum)))

s3 = boto3.client('s3')
response = s3.get_object(Bucket='aws-glue-assets-613158767627-eu-central-1', Key='scripts/data/_periodicityIRIs.json')
content = response['Body'].read().decode('utf-8')
periodicity_iris = json.loads(content)

def createPeriodicityPredicate(graph, rootNode, periodicity):
    # Verify pre-conditions
    if type(graph) is not Graph:
        raise TypeError ("Parameter \"graph\" must be a RDFLIB Graph()")
    if type(rootNode) is not URIRef:
        raise TypeError ("Parameter \"rootNode\" must be a RDFLIB URIRef()")
    if type(periodicity) is not str:
        raise TypeError ("Parameter \"periodicity\" must be a string:[" + str(periodicity) + "]")
    if (len(periodicity) < 1):
        raise TypeError ("Parameter \"periodicity\" must have content:[" + periodicity + "]")
        
    valueKey = periodicity.replace(" ","").lower()
    valueIRI = "UNKNOWN"
    if (valueKey in periodicity_iris):
        valueIRI = periodicity_iris[valueKey]
    
    graph.add((rootNode, DCTERMS.accrualPeriodicity, URIRef(valueIRI)))
    
    return True

s3 = boto3.client('s3')
response = s3.get_object(Bucket='aws-glue-assets-613158767627-eu-central-1', Key='scripts/data/_siecIRIs.json')
content = response['Body'].read().decode('utf-8')
siec_iris = json.loads(content) 

def createSiecPredicate(graph, rootNode, siec):
    # Verify pre-conditions
    if type(graph) is not Graph:
        raise TypeError ("Parameter \"graph\" must be a RDFLIB Graph()")
    if type(rootNode) is not URIRef:
        raise TypeError ("Parameter \"rootNode\" must be a RDFLIB URIRef()")
    if type(siec) is not str:
        raise TypeError ("Parameter \"SIEC\" must be a string:[" + str(siec) + "]")
    if (len(siec) < 1):
        raise TypeError ("Parameter \"SIEC\" must have content:[" + siec + "]")
    
    valueKey = siec.replace(" ","").lower()
    valueIRI = "UNKNOWN"
    if (valueKey in siec_iris):
        valueIRI = siec_iris[valueKey]
    
    graph.add((rootNode, BUILTHUB.siec, URIRef(valueIRI)))
    
    return True

class BuiltHubDB:
    __neptuneSPARQLEntrypoint = r"https://builthub-graphdb.cluster-ro-ccifguf2pv5z.eu-central-1.neptune.amazonaws.com:8182/sparql"

    def __init__(self):
        self.__http = urllib3.PoolManager()
    
    def executeQuery(self, query):
        params = {"query": query}
        encoded_params = urlencode(params)

        response = self.__http.request("POST", self.__neptuneSPARQLEntrypoint + "?" + encoded_params, headers = {"Content-Type" : "application/octet-stream", "Accept" : "application/json", "Accept-Charset" : "UTF-8"} )
        if (response.status != 200):
            raise BuiltHubException(response.data.decode("UTF-8"))
        
        raw  = response.data.decode("UTF-8")

        return raw

    def findLocation(self, locationName):
        query = r"PREFIX gn: <http://www.geonames.org/ontology#> PREFIX gns: <https://www.geonames.org/ontology#> SELECT DISTINCT ?s WHERE { ?s a gn:Feature . ?s gn:featureClass gns:P . ?s gn:name ?name . FILTER (regex(?name, '" + locationName + "', 'i'))} LIMIT 1"

        raw = self.executeQuery(query)
        rawJson = json.loads(raw)
        results = rawJson["results"]["bindings"]
        if (len(results) == 0):
            return None

        uri = results[0]["s"]["value"]

        return URIRef(uri)


    