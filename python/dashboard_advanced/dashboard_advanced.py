import psycopg2
from SPARQLWrapper import SPARQLWrapper2


#Conexion Graph
sparql = SPARQLWrapper2("http://localhost7200/repositories/BuiltHub")


#Query
sparql.setQuery("""
PREFIX cbhsv: <http://data.builthub.eu/ontology/cbhsv#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX at: <http://publications.europa.eu/ontology/authority/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT DISTINCT ?identifier ?NutsId ?measuredElement ?LocationId ?datasetID ?btype ?msrValue ?period
WHERE {
  {
        ?v a cbhsv:Dataset017 ;
        dc:identifier ?identifier ;
        dct:temporal/dcat:startDate ?startDate ;
        dct:temporal/dcat:endDate ?endDate ;
        dcat:spatial/skos:prefLabel ?location ;
        dcat:spatial/at:authority-code ?LocationId ;
        cbhsv:hasNUTS/dc:identifier ?NutsId ;
        cbhsv:measuredElement ?measuredElement;
        cbhsv:btype ?btype ;
        cbhsv:measurementValue ?msrValue ;
    BIND(concat(concat(str(year(?startDate)), '-'), str(year(?endDate))) as ?period)
    BIND('17'^^xsd:integer as ?datasetID)
    FILTER (lang(?location) = 'en')
  } UNION {
    ?s  a cbhsv:Dataset023 ;
        dc:identifier ?identifier ;
        dct:temporal/dcat:startDate ?startDate ;
        dct:temporal/dcat:endDate ?endDate ;
        dcat:spatial/skos:prefLabel ?location ;
        dcat:spatial/at:authority-code ?LocationId ;
        cbhsv:hasNUTS/dc:identifier ?NutsId ;
        cbhsv:measuredElement ?measuredElement ;
        cbhsv:measurementValue ?msrValue ;
        BIND(concat(concat(str(year(?startDate)), '-'), str(year(?endDate))) as ?period)
    BIND('23'^^xsd:integer as ?datasetID)
    BIND('' as ?btype)
    FILTER (lang(?location) = 'en')
    FILTER(!STRSTARTS(?NutsId, "NAP") || !STRSTARTS(?NutsLvl, "9999"))
    FILTER regex(?NutsId, "^[A-Z]{2}+$|^[A-Z]{2}[A-Y0-9]{1,3}")
  }
}
    """)


#Conexion Postgres
connection = psycopg2.connect("dbname=builthubdb user=user password=password host=localhost")
cursor = connection.cursor()


#DELETE Data
cursor.execute("DELETE FROM \"public\".\"dashboard_advanced\"")
connection.commit()


#INSERT Data
for result in sparql.query().bindings:
    if(result["NutsId"].value=="BE221" or result["NutsId"].value=="BE222" or result["NutsId"].value=="BE321" or result["NutsId"].value=="BE322" or result["NutsId"].value=="BE324" or result["NutsId"].value=="BE325" or result["NutsId"].value=="BE326" or result["NutsId"].value=="BE327" or result["NutsId"].value=="EE006" or result["NutsId"].value=="EE007" or result["NutsId"].value=="HR041" or result["NutsId"].value=="HR042" or result["NutsId"].value=="HR043" or result["NutsId"].value=="HR044" or result["NutsId"].value=="HR045" or result["NutsId"].value=="HR046" or result["NutsId"].value=="HR047" or result["NutsId"].value=="HR048" or result["NutsId"].value=="HR049" or result["NutsId"].value=="HR04A" or result["NutsId"].value=="HR04B" or result["NutsId"].value=="HR04C" or result["NutsId"].value=="HR04D" or result["NutsId"].value=="HR04E" or result["NutsId"].value=="HR04" or result["NutsId"].value=="ITG25" or result["NutsId"].value=="ITG26" or result["NutsId"].value=="ITG27" or result["NutsId"].value=="ITG28" or result["NutsId"].value=="ITG29" or result["NutsId"].value=="ITG2A" or result["NutsId"].value=="ITG2B" or result["NutsId"].value=="ITG2C" or result["NutsId"].value=="UKK21" or result["NutsId"].value=="UKK22"):
        continue
    try:
        cursor.execute("INSERT INTO \"public\".\"dashboard_advanced\" (\"nuts_code\", \"measuredelement\", \"location_id\", \"dataset_id\", \"btype\", \"msrvalue\", \"period\") VALUES ('"+result["NutsId"].value+"','"+result["measuredElement"].value+"','"+result["LocationId"].value+"','"+result["datasetID"].value+"','"+result["btype"].value+"',"+result["msrValue"].value+",'"+result["period"].value+"')")
        connection.commit()
    except:
        print("Error with data: "+result)
cursor.close()
connection.close()

print("DONE")