from elasticsearch import Elasticsearch
from py2neo import Graph, Node, Relationship
# , Database

ng=200

#the index client used to communicate with the database
client = Elasticsearch([{"host": "127.0.0.1", "scheme": "http", "port": 9200}])
#the index name
indexName = "gastronomical"
docType = 'recipes'

# Graph database entity
# graph_db = Graph("bolt://localhost:7687", auth=('neo4j', 'iu6-magisters'))
graph_db = Graph("bolt://localhost:7687")
#print Database.kernel_version
#print Database.name
#print Database.uri

# graph_db.delete_all()

#Ingredients data
# ingredients
filename = 'ingredients.txt'
ingredients =[]
with open(filename, 'r') as f:
    for line in f:
        # strip because of the /n you get otherwise from reading the .txt
        ingredients.append(line.strip())

#print ingredients

# ElasticSearch to Neo4J 

ingredientnumber = 0
grandtotal = 0
i=0
i1=0
j=0
for ingredient in ingredients:
    try:
         IngredientNode = Node("Ingredient",Name=ingredient)
         graph_db.create(IngredientNode)
    except Exception as e:
        print(e)
        continue

    ingredientnumber +=1
    searchbody = {
        "size": 10000,
        "query": {
            "match_phrase":
                {
                    "ingredients":{
                        "query":ingredient
                    }
                }
        }
    }

    result = client.search(index=indexName, body=searchbody)

    print("begin load")
    
   # print ingredient
   # print ingredientnumber
   #  print "total: " +  str(result['hits']['total'][ 'value'])

   #  grandtotal = grandtotal + result['hits']['total'][ 'value']
   # print "grand total: " +  str(grandtotal)

    for recipe in result['hits']['hits']:
        print("+", end="")
        try:
            RecipeNode=graph_db.nodes.match("Recipe", Name=
                       recipe['_source']['name']).first()
            if  RecipeNode==None:
                RecipeNode = Node("Recipe",Name=recipe['_source']['name'])
                graph_db.create(RecipeNode)
            NodesRelationship = Relationship(RecipeNode, "Contains", IngredientNode)
            graph_db.create(NodesRelationship)
   #       print "added: " + recipe['_source']['name'] + " contains " + ingredient
        except Exception as e:
            print(e)
            continue
    i+=1
    j+=1
    if i>=10:
         i1+=i
         print (" ingredient: " + str(i1) +" from "+ str(ng))
         i=0
    if j>=ng:
         break
    print ("*************************************")

