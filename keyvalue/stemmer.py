import site
import sys

site.addsitedir('/usr/local/lib/python3.7/site-packages')  # Always appends to end

from nltk.stem import PorterStemmer 

def stem(word):
    stemmer = PorterStemmer()
    return stemmer.stem(word)
