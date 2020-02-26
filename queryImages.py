import keyvalue.sqlitekeyvalue as KeyValue
import keyvalue.parsetriples as ParseTripe
import keyvalue.stemmer as Stemmer
import sys, getopt


def searchWords(args):

    # Make connections to KeyValue
    kv_labels = KeyValue.SqliteKeyValue("sqlite_labels.db","labels",sortKey=True)
    kv_images = KeyValue.SqliteKeyValue("sqlite_images.db","images")

    args = args.split()
    for arg in args:
        print(arg)
        arg = Stemmer.stem(arg)
        labels = kv_labels.getAll(arg)
        if(len(labels) == 0):
            print('Label not found')
        else:
            for label in labels:
                #print(label[0])
                image = kv_images.get(label[0])
                print(image)
        

    # Close KeyValues Storages
    kv_labels.close()
    kv_images.close()



# Process Logic.
def main(argv):
    try:
        opts, args = getopt.getopt(argv, "hl:")
    except:
        print('queryImages.py -l <labels>')
        sys.exit(2)
    
    if(len(opts) == 0):
        print('usage: queryImages.py -l <labels>')
        sys.exit()

    for opt, arg in opts:
        if opt == '-h':
            print('queryImages.py -l <labels separated by space>')
            sys.exit()
        elif opt == '-l':
            searchWords(arg)
    


if __name__ == "__main__":
    main(sys.argv[1:])






