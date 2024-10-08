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
SELECT DISTINCT ?identifier ?nutsID ?datasetID ?measuredElement ?location_id ?msrValue ?period ?msrUnit ?siec ?ttype ?subsector ?sector ?btype
WHERE {
  {
    SELECT ?identifier ?nutsID ('14'^^xsd:integer as ?datasetID) ?measuredElement ?location_id ?msrValue ?period ?msrUnit ?siec 
    WHERE{
        ?s      a cbhsv:Dataset014 ;
            cbhsv:hasNUTS/ns72:level '0.0'^^xsd:decimal ;
            dc:identifier ?identifier ;
            dct:temporal/dcat:startDate ?startDate ;
            dcat:spatial/skos:prefLabel ?location ;
            dcat:spatial/at:authority-code ?location_id ;
            cbhsv:hasNUTS/dc:identifier ?nutsID ;
            cbhsv:measuredElement ?measuredElement;
            cbhsv:siec/skos:notation ?siec ;
            cbhsv:measurementUnit ?msrUnit;
            cbhsv:measurementValue ?msrValue .
        FILTER (lang(?location) = 'en')
        BIND(str(year(?startDate)) as ?period)
    }
  } UNION {
    SELECT ?identifier ?nutsID ('15'^^xsd:integer as ?datasetID) ?measuredElement ?location_id ?msrValue ?period ?msrUnit ?siec
    WHERE{
        ?s      a cbhsv:Dataset015 ;
            cbhsv:hasNUTS/ns72:level '0.0'^^xsd:decimal ;
            dc:identifier ?identifier ;
            dct:temporal/dcat:startDate ?startDate ;
            dcat:spatial/skos:prefLabel ?location ;
            dcat:spatial/at:authority-code ?location_id ;
            cbhsv:hasNUTS/dc:identifier ?nutsID ;
            cbhsv:measuredElement ?measuredElement;
            cbhsv:siec/skos:notation ?siec ;
            cbhsv:measurementUnit ?msrUnit;
            cbhsv:measurementValue ?msrValue .
        FILTER (lang(?location) = 'en')
        BIND(str(year(?startDate)) as ?period)
    }
  } UNION {
    SELECT ?identifier ?nutsID ('16'^^xsd:integer as ?datasetID) ?measuredElement ?location_id ?msrValue ?period ?msrUnit ?siec 
    WHERE{
        ?s      a cbhsv:Dataset016 ;
            cbhsv:hasNUTS/ns72:level '0.0'^^xsd:decimal ;
            dc:identifier ?identifier ;
            dct:temporal/dcat:startDate ?startDate ;
            dcat:spatial/skos:prefLabel ?location ;
            dcat:spatial/at:authority-code ?location_id ;
            cbhsv:hasNUTS/dc:identifier ?nutsID ;
            cbhsv:measuredElement ?measuredElement;
            cbhsv:siec/skos:notation ?siec ;
            cbhsv:measurementUnit ?msrUnit;
            cbhsv:measurementValue ?msrValue .
        FILTER (lang(?location) = 'en')
        BIND(str(year(?startDate)) as ?period)
    }
  } UNION {
    SELECT DISTINCT ?nutsID ('1'^^xsd:integer as ?datasetID) ?measuredElement ?location_id ?msrValue ?period ?msrUnit ?ttype ?subsector ?sector ?btype
    WHERE {
        ?s      a cbhsv:Dataset001 ;
                cbhsv:hasNUTS/ns72:level '0.0'^^xsd:decimal ;
                cbhsv:topic <http://data.builthub.eu/resource/set/Topic/Energy> ;
                dct:temporal/dcat:startDate ?startDate ;
                dct:temporal/dcat:endDate ?endDate ;
                dcat:spatial/skos:prefLabel ?location ;
                dcat:spatial/at:authority-code ?location_id ;
                cbhsv:hasNUTS/dc:identifier ?nutsID ;
                cbhsv:hasNUTS/ns72:level ?nutsLevel ;
                cbhsv:sector/skos:prefLabel ?sector ;
                cbhsv:subsector/skos:prefLabel ?subsector ;
                cbhsv:type/skos:prefLabel ?ttype ;
                cbhsv:btype/skos:prefLabel ?btype ;
                cbhsv:feature/skos:prefLabel ?measuredElement ;
                cbhsv:measurementUnit ?msrUnit;
                cbhsv:measurementValue ?msrValue .
        BIND(concat(concat(str(year(?startDate)), '-'), str(year(?endDate))) as ?period)
        FILTER (lang(?location) = 'en')
    }
  }
}
    """)


#Conexion Postgres
connection = psycopg2.connect("dbname=builthubdb user=user password=password host=localhost")
cursor = connection.cursor()


#DELETE Data
cursor.execute("DELETE FROM \"public\".\"dashboard_energy\"")
connection.commit()


#INSERT Data
for result in sparql.query().bindings:
    try:
        if(result["siec"].value!="" or result["siec"].value!=None):
            cursor.execute("INSERT INTO \"public\".\"dashboard_energy\" (\"nuts_code\", \"measuredelement\", \"dataset_id\", \"location_id\", \"msrvalue\", \"period\", \"msrunit\", \"siec_id\") VALUES ('"+result["nutsID"].value+"','"+result["measuredElement"].value+"','"+result["datasetID"].value+"','"+result["location_id"].value+"',"+result["msrValue"].value+",'"+result["period"].value+"','"+result["msrUnit"].value+"','"+result["siec"].value+"')")
            connection.commit()
    except:
        try:
            cursor.execute("INSERT INTO \"public\".\"dashboard_energy\" (\"nuts_code\", \"ttype\", \"measuredelement\", \"dataset_id\", \"subsector\", \"location_id\", \"msrvalue\", \"sector\", \"period\", \"btype\", \"msrunit\") VALUES ('"+result["nutsID"].value+"','"+result["ttype"].value+"','"+result["measuredElement"].value+"','"+result["datasetID"].value+"','"+result["subsector"].value+"','"+result["location_id"].value+"',"+result["msrValue"].value+",'"+result["sector"].value+"','"+result["period"].value+"','"+result["btype"].value+"','"+result["msrUnit"].value+"')")
            connection.commit()
        except:
            print("Error with data: "+result)
cursor.close()
connection.close()

print("DONE")