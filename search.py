import time
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
from constants import *
from math import log
from heapq import heapify, heappush, heappop, heappushpop
from wordProcessing import *

regex_tokenizer = RegexpTokenizer("[\w']+")
stopwords_list = stopwords.words('english')
articles_count = 0

def get_word_offset(word):
	first_char_word = word[0]
	with open(offset_dir + "/" + first_char_word + ".txt", "r") as f:
		lines = f.readlines()
		lo = 0
		hi = len(lines) - 1
		word_offset = -1
		while hi >= lo:
			mid = (hi + lo) // 2
			offset, curr_word = lines[mid].split()
			#print(offset, curr_word, word)
			if curr_word == word:
				word_offset = int(offset)
				#print(offset)
				break
			elif curr_word > word:
				hi = mid - 1
			else:
				lo = mid + 1

		return word_offset

	

def get_word_postings(word, word_offset):
	first_char_word = word[0]
	with open(index_dir + "/" + first_char_word + ".txt", "r") as f:
		f.seek(word_offset)
		line = f.readline()
		print(line)
		tokens = line.split()
		idf = get_idf_score(len(tokens) // 2)
		length = min(len(tokens) - 1, 2 * top_n_docs_for_each_word)
		postings = []
		for i in range(1, length, 2):
			term_freq = int(tokens[i + 1])
			tf_idf = get_tf_idf(term_freq, idf)
			postings.append([int(tokens[i]), tf_idf])

		return postings



def get_idf_score(word_in_articles):
	return log(articles_count / word_in_articles)


def get_tf_idf(term_freq, idf):
	tf = log(1 + term_freq)
	return round(tf * idf, 2)

def get_top_n_documents(word_postings):
	doc_scores = {}
	for posting in word_postings:
		#print(len(posting))
		for doc_id, tf_idf in posting:
			#print(doc_id, tf_idf)
			if doc_id in doc_scores:
				doc_scores[doc_id] = round(doc_scores[doc_id] + tf_idf, 2)
			else:
				doc_scores[doc_id] = tf_idf
		#print()

	#print(len(doc_scores))
	keys = list(doc_scores.keys())
	top_n_docs = []
	for i in range(0, min(top_n_docs_len, len(keys))):
		key = keys[i]
		top_n_docs.append((doc_scores[key], key))

	heapify(top_n_docs)

	if i < len(keys):
		for i in range(top_n_docs_len, len(keys)):
			key = keys[i]
			if doc_scores[key] > top_n_docs[0][0]:
				heappushpop(top_n_docs, (doc_scores[key], key))

	ranked_docs = []
	heap_size = len(top_n_docs)
	for i in range(heap_size):
		tf_idf_score, doc_id = heappop(top_n_docs)
		ranked_docs.append((wikipedia_url + str(doc_id), tf_idf_score))
	ranked_docs.reverse()

	return ranked_docs

def get_articles_count():
	with open(partial_index_articles_count_dir + "/total-articles.txt") as f:
		articles_count = int(f.readline())

	return articles_count

if __name__ == "__main__":
	while True:
		query = input("Enter query: ")
		print("Query: " + query)
		start_time = time.time()
		query = replace_punctuations(query)
		tokens = regex_tokenizer.tokenize(query)
		word_postings = []
		articles_count = get_articles_count()
		for word in tokens:
			word = get_processed(word)
			print("Word:"  + word)
			if word in stopwords_list:
				continue
			word_offset = get_word_offset(word)
			if word_offset != -1:
				postings = get_word_postings(word, word_offset)

				word_postings.append(postings)
			else:
				print("Word {0} not found".format(word))

		#print(word_postings)
		if len(word_postings) > 0:
			top_n_docs = get_top_n_documents(word_postings)
			for doc in top_n_docs:
				print(doc)
		else:
			print("Invalid query")

		print("Query time: {0}".format(round(time.time() - start_time, 4)))


