import json
import os
import time
from nltk.corpus import stopwords
from nltk.tokenize import LineTokenizer, RegexpTokenizer
from pyspark import SparkContext, SparkConf
from collections import OrderedDict
from constants import *
from wordProcessing import *

start_time = time.time()
stopwords_list = stopwords.words('english')
line_tokenizer = LineTokenizer()
regex_tokenizer = RegexpTokenizer("[\w']+")

def extract(article):
	if article == None or article == '':
  		print('I got a null or empty string value for article in a file')
  		return {}
	obj = json.loads(article)
	article_id = int(obj['id'])
	text = obj['text']
	
	text = replace_punctuations(text)
	text = strip_accents(text)

	lines = line_tokenizer.tokenize(text)
	word_map = {}
	for line in lines:
		#print(line)
		words = regex_tokenizer.tokenize(line)
		for word in words:
			word = get_processed(word)
			if word in stopwords_list:
				continue
			if word in word_map:
				word_count = word_map[word][0][1]
				word_map[word] = [(article_id, word_count + 1)]
			else:
				word_map[word] = [(article_id, 1)]

	return word_map


def sortDesc(tupl):
	return -tupl[1]


def aggregate(a, b):
	for key in a:
		if key in b:
			l = sorted(a[key] + b[key], key=sortDesc)
			b[key] = l
		else:
			b[key] = a[key]
	return OrderedDict(sorted(b.items()))


def get_article_count(directory):
	article_count = 0
	for file in os.listdir(directory):
		with open(directory + "/" + file, "r") as f:
			article_count += int(f.readline())

	return article_count
 
if __name__ == "__main__":
 
	sc = SparkContext()

	if not os.path.exists(partial_index_dir):
		os.makedirs(partial_index_dir)
		os.makedirs(partial_index_articles_count_dir)


	for path, subdirs, files in os.walk(wiki_extract_dir):
		for name in files:
			file_name = os.path.join(path, name)
			inputRdd = sc.textFile(file_name)
			#print("{0} {1}".format(file_name, str(inputRdd.count())))
			with open(partial_index_articles_count_dir + "/" + name, "w") as f:
				f.write(str(inputRdd.count()))
			mapRdd = inputRdd.map(extract)
			wordDict = mapRdd.reduce(aggregate)

			keys = list(wordDict)
			with open(partial_index_dir + "/" + name, "a") as f:
				for key in keys:
					flatlist = [str(item) for sublist in wordDict[key] for item in sublist]
					list_str = ' '.join(flatlist)
					line_str = key + " " + list_str + "\n"
					f.write(line_str)

	articles_count = get_article_count(partial_index_articles_count_dir)
	with open(partial_index_articles_count_dir + "/total-articles.txt", "w") as f:
		f.write(str(articles_count))

	print("--- %s seconds ---" % (time.time() - start_time))


