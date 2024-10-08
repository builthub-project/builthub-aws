import psycopg2
from SPARQLWrapper import SPARQLWrapper2


#Conexion Graph
sparql = SPARQLWrapper2("http://localhost:7200/repositories/BuiltHub")


#Query
sparql.setQuery("""
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX cbhsv: <http://data.builthub.eu/ontology/cbhsv#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX at: <http://publications.europa.eu/ontology/authority/>
SELECT DISTINCT ?identifier ?NutsId ?LocationId ('23'^^xsd:integer as ?datasetID) ?startDate ?endDate ?msrValue ?measuredElement ?unit WHERE {
    ?s      a cbhsv:Dataset023 ;
        dc:identifier ?identifier ;
        dct:temporal/dcat:startDate ?startDate ;
        dct:temporal/dcat:endDate ?endDate ;
        dcat:spatial/skos:prefLabel ?location ;
        dcat:spatial/at:authority-code ?LocationId ;
        cbhsv:hasNUTS/dc:identifier ?NutsId ;
        cbhsv:measuredElement ?measuredElement ;
        cbhsv:measurementUnit ?unit ;
        cbhsv:measurementValue ?msrValue .
    FILTER (lang(?location) = 'en')
    FILTER(!STRSTARTS(?NutsId, "NAP") || !STRSTARTS(?NutsLvl, "9999"))
    FILTER regex(?NutsId, "^[A-Z]{2}+$|^[A-Z]{2}[A-Y0-9]{1,3}")
}
    """)


#Conexion Postgres
connection = psycopg2.connect("dbname=builthubdb user=user password=password host=localhost")
cursor = connection.cursor()


#DELETE Data
cursor.execute("DELETE FROM \"public\".\"dataset023\"")
connection.commit()

cursor.execute("DELETE FROM \"public\".\"dataset_period\" WHERE dataset_id = 23")
connection.commit()


#INSERT Data
for result in sparql.query().bindings:
    try:
        cursor.execute("INSERT INTO \"public\".\"dataset_period\" (\"dataset_id\", \"record_id\", \"start_period\", \"end_period\") VALUES ('"+result["datasetID"].value+"','"+result["identifier"].value+"','"+result["startDate"].value+"','"+result["endDate"].value+"')")
        connection.commit()

        cursor.execute("INSERT INTO \"public\".\"dataset023\" (\"record_id\", \"nuts_code\", \"location_id\", \"dataset_id\", \"msrvalue\", \"measuredelement\", \"msrunit\", \"flags\") VALUES ('"+result["identifier"].value+"','"+result["NutsId"].value+"','"+result["LocationId"].value+"','"+result["datasetID"].value+"',"+result["msrValue"].value+",'"+result["measuredElement"].value+"','"+result["unit"].value+"','None')")
        connection.commit()
    except:
        print("Error with data: "+result)
cursor.close()
connection.close()

print("DONE")