from elasticsearch import Elasticsearch
import json

ne=50000

#the index name Elasticsearch client used to communicate with the database
client = Elasticsearch([{"host": "127.0.0.1", "scheme": "http", "port": 9200}])
indexName = "gastronomical"
docType = "recipes"
# create an index (only once)

# client.indices.delete(index=indexName)
# client.indices.create(index=indexName)

print("created")

# location of recipe json file: change this to match your own setup!
#Create document mapping
recipeMapping = {
        "properties": {
            "name": {"type": "text"},
            "ingredients": {"type": "text"}
        }
    }
client.indices.put_mapping(index=indexName,
                           # doc_type=docType,  
                           # include_type_name="true", 
                           body=recipeMapping
                          )

print("mapped")

#Load Json file
with open('recipes.json', 'r', encoding='utf-8') as data_file:
    recipeData = json.load(data_file)
#Index the recipes
i=0
i1=0
j=0
for recipe in recipeData:
    print("+", end="")
#    print recipe.keys()
#    print recipe['_id'].keys()
    try: 
        client.index(
            index=indexName, 
            # doc_type=docType,
            id = recipe['_id']['$oid'], 
            body={"name": recipe['name'],"ingredients":recipe['ingredients']})
        i+=1
        j+=1
        if i>=1000:
             i1+=i
             print ("index: " + str(i1) +" from "+ str(ne))
             i=0
        if j>=ne:
             break            
    except Exception as e:
        print(e)

