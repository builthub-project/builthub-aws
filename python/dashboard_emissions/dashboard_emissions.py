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
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX at: <http://publications.europa.eu/ontology/authority/>
SELECT DISTINCT ?identifier ?locationID ?nutsID ?sector ?measuredElement ?period ?msrValue ('28'^^xsd:integer as ?datasetID) WHERE {
    ?s      a cbhsv:Dataset028 ;
    cbhsv:hasNUTS/ns72:level '0.0'^^xsd:decimal ;
    dc:identifier ?identifier ;
    dct:temporal/dcat:startDate ?startDate ;
    dcat:spatial/skos:prefLabel ?location ;
    dcat:spatial/at:authority-code ?locationID ;
    cbhsv:hasNUTS/dc:identifier ?nutsID ;
    cbhsv:measuredElement ?measuredElement ;
    cbhsv:sector ?sector ;
    cbhsv:measurementValue ?msrValue .
    BIND(str(year(?startDate)) as ?period)
    FILTER (lang(?location) = 'en')
}
    """)


#Conexion Postgres
connection = psycopg2.connect("dbname=builthubdb user=user password=password host=localhost")
cursor = connection.cursor()


#DELETE Data
cursor.execute("DELETE FROM \"public\".\"dashboard_emissions\"")
connection.commit()


#INSERT Data
for result in sparql.query().bindings:
    try:
        cursor.execute("INSERT INTO \"public\".\"dashboard_emissions\" (\"nuts_code\", \"msrvalue\", \"dataset_id\", \"location_id\", \"sector\", \"measuredelement\", \"period\") VALUES ('"+result["nutsID"].value+"',"+result["msrValue"].value+",'"+result["datasetID"].value+"','"+result["locationID"].value+"','"+result["sector"].value+"','"+result["measuredElement"].value+"','"+result["period"].value+"')")
        connection.commit()
    except:
        print("Error with data: "+result)
cursor.close()
connection.close()

print("DONE")