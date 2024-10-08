import psycopg2
from SPARQLWrapper import SPARQLWrapper2


#Conexion Graph
sparql = SPARQLWrapper2("http://localhost:7200/repositories/BuiltHub")


#Query
sparql.setQuery("""
PREFIX cbhsv: <http://data.builthub.eu/ontology/cbhsv#>
PREFIX ns72: <http://data.europa.eu/nuts/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX at: <http://publications.europa.eu/ontology/authority/>
SELECT DISTINCT ?identifier ?nutsID ?locationID ?datasetID ?sector ?measuredElement ?period ((?msrValue1/?msrValue2) as ?msrValue) ?msrUnit
WHERE {
    {
        ?s a cbhsv:Dataset005 ;
            cbhsv:hasNUTS/ns72:level '0.0'^^xsd:decimal ;
            dc:identifier ?identifier ;
            dct:temporal/dcat:startDate ?startDate ;
            dcat:spatial/skos:prefLabel ?location ;
            dcat:spatial/at:authority-code ?locationID ;
            cbhsv:hasNUTS/dc:identifier ?nutsID ;
            cbhsv:measuredElement ?measuredElement ;
            cbhsv:sector ?sector ;
            cbhsv:measurementValue ?msrValue1 .
        BIND(str(year(?startDate)) as ?period)
        BIND("kWh/individual" as ?msrUnit)
        BIND('5'^^xsd:integer as ?datasetID)
        FILTER (lang(?location) = 'en')
        FILTER(?locationID=?locationID2)
    }
    {
    SELECT DISTINCT ?locationID2 ?location2 ?measuredElement2 ?period2 ?msrValue2
    WHERE {
        ?s a cbhsv:Dataset501 ;
            dct:temporal/dcat:startDate ?startDate2 ;
            dc:identifier ?identifier ;
            dcat:spatial/skos:prefLabel ?location2 ;
            dcat:spatial/at:authority-code ?locationID2 ;
            cbhsv:measuredElement ?measuredElement2 ;
            cbhsv:measurementValue ?msrValue2 .
        BIND(str(year(?startDate2)) as ?period2)
        BIND('501'^^xsd:integer as ?datasetID)
        FILTER (lang(?location2) = 'en')
        FILTER(?measuredElement2='Average persons by households')
        FILTER(?period2='2001')
    }
    }
}
    """)


#Conexion Postgres
connection = psycopg2.connect("dbname=builthubdb user=user password=password host=localhost")
cursor = connection.cursor()


#DELETE Data
cursor.execute("DELETE FROM \"public\".\"dashboard_comparison\"")
connection.commit()


#INSERT Data
for result in sparql.query().bindings:
    try:
        cursor.execute("INSERT INTO \"public\".\"dashboard_comparison\" (\"dashboard_id\", \"nuts_code\", \"location_id\", \"dataset_id\", \"sector\", \"measuredelement\", \"period\", \"msrvalue\", \"msrunit\") VALUES ('"+result["identifier"].value+"','"+result["nutsID"].value+"','"+result["locationID"].value+"','"+result["datasetID"].value+"','"+result["sector"].value+"','"+result["measuredElement"].value+"','"+result["period"].value+"',"+result["msrValue"].value+",'"+result["unit"].value+"')")
        connection.commit()
    except:
        print("Error with data: "+result)
cursor.close()
connection.close()

print("DONE")