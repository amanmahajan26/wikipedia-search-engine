import unicodedata
import string
from nltk.stem import PorterStemmer

porter = PorterStemmer()

def strip_accents(text):

    try:
        text = unicode(text, 'utf-8')
    except NameError: # unicode is a default on python 3 
        pass

    text = unicodedata.normalize('NFD', text)\
           .encode('ascii', 'ignore')\
           .decode("utf-8")

    return str(text)

def get_processed(word):
	'''
	- lowercase
	- stem
	'''
	return porter.stem(word.lower())


def replace_punctuations(text):
	# replace punctuations
	specialChar = '–' # one of the hyphen chars used in wikipedia
	charsToBeReplaced = specialChar + string.punctuation
	charsToBeReplacedWith = ' ' * len(charsToBeReplaced)
	charsToBeDeleted = ''
	table = str.maketrans(charsToBeReplaced, charsToBeReplacedWith, charsToBeDeleted)
	
	return text.translate(table)