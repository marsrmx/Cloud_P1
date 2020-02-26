import keyvalue.sqlitekeyvalue as KeyValue
import keyvalue.parsetriples as ParseTripe
import keyvalue.stemmer as Stemmer

# Make connections to KeyValue
kv_labels = KeyValue.SqliteKeyValue("sqlite_labels.db","labels",sortKey=True)
kv_images = KeyValue.SqliteKeyValue("sqlite_images.db","images")

# Process Algorithm.

#Read Images and Labels from the file
parseTriplesImages = ParseTripe.ParseTriples("images.ttl")
parseTriplesLabels = ParseTripe.ParseTriples("labels_en.ttl")

#Parse Images to the Image Collection
#Only the first 1000 images

countImages = 0
while(countImages <= 1000):
    triple = parseTriplesImages.getNext()
    #print(triple)
    if(triple[1] == 'http://xmlns.com/foaf/0.1/depiction'):
        kv_images.put(triple[0], triple[2])
        countImages += 1

#Parse Labels to the Label Collection
#Store sortKey per keys
terms = {}

#Get the first label
label = parseTriplesLabels.getNext()
while(label):
    if(kv_images.get(label[0]) and label[1] == 'http://www.w3.org/2000/01/rdf-schema#label'):
        labels = label[2].split()
        for l in labels:
            l = Stemmer.stem(l)
            if(terms.get(l) is None):
                terms[l] = 1
            else:
                terms[l] +=1
            kv_labels.putSort(l, str(terms[l]), label[0])
            #print(label[0])
    label = parseTriplesLabels.getNext()

#To verify
#print(terms)

#Utrecht example of a label with two articles

# Close KeyValues Storages
kv_labels.close()
kv_images.close()









