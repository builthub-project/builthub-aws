import psycopg2
from SPARQLWrapper import SPARQLWrapper2


#Conexion Graph
sparql = SPARQLWrapper2("http://localhost:7200/repositories/BuiltHub")


#Query
sparql.setQuery("""
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX cbhsv: <http://data.builthub.eu/ontology/cbhsv#>
PREFIX ns72: <http://data.europa.eu/nuts/>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX ns71: <http://purl.org/dc/terms/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX at: <http://publications.europa.eu/ontology/authority/>
SELECT DISTINCT ?identifier ?nutsID ?location_id ('14'^^xsd:integer as ?datasetID) ?siec ?freq ?startDate ?endDate ?msrValue ?measuredElement ?msrUnit WHERE {
    ?s      a cbhsv:Dataset014 ;
        cbhsv:hasNUTS/ns72:level '0.0'^^xsd:decimal ;
        dc:identifier ?identifier ;
        ns71:accrualPeriodicity/rdfs:label ?freq ;
        dct:temporal/dcat:startDate ?startDate ;
        dct:temporal/dcat:endDate ?endDate ;
        dcat:spatial/skos:prefLabel ?location ;
        dcat:spatial/at:authority-code ?location_id ;
        cbhsv:hasNUTS/dc:identifier ?nutsID ;
        cbhsv:measuredElement ?measuredElement;
        cbhsv:siec/skos:notation ?siec ;
        cbhsv:measurementUnit ?msrUnit;
        cbhsv:measurementValue ?msrValue .
    FILTER (lang(?location) = 'en')
}
    """)


#Conexion Postgres
connection = psycopg2.connect("dbname=builthubdb user=user password=password host=localhost")
cursor = connection.cursor()


#DELETE Data
cursor.execute("DELETE FROM \"public\".\"dataset014\"")
connection.commit()

cursor.execute("DELETE FROM \"public\".\"dataset_period\" WHERE dataset_id = 14")
connection.commit()


#INSERT Data
for result in sparql.query().bindings:
    try:
        cursor.execute("INSERT INTO \"public\".\"dataset_period\" (\"dataset_id\", \"record_id\", \"start_period\", \"end_period\") VALUES ('"+result["datasetID"].value+"','"+result["identifier"].value+"','"+result["startDate"].value+"','"+result["endDate"].value+"')")
        connection.commit()

        cursor.execute("INSERT INTO \"public\".\"dataset014\" (\"record_id\", \"nuts_code\", \"location_id\", \"dataset_id\", \"siec\", \"frequency\", \"msrvalue\", \"measuredelement\", \"msrunit\") VALUES ('"+result["identifier"].value+"','"+result["nutsID"].value+"','"+result["location_id"].value+"','"+result["datasetID"].value+"','"+result["siec"].value+"','"+result["freq"].value+"',"+result["msrValue"].value+",'"+result["measuredElement"].value+"','"+result["msrUnit"].value+"')")
        connection.commit()
    except:
        print("Error with data: "+result)
cursor.close()
connection.close()

print("DONE")