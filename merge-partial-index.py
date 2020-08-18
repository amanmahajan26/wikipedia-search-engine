import os
import time
from heapq import heapify, heappush, heappop
from constants import *

start_time = time.time()

def pop_and_push_next(heap, read_files):
	if len(heap) == 0:
		return None
	tup = heappop(heap)
	index = tup[2]
	line = read_files[index].readline()
	if line != '':
		word, freq_map = line_to_tuple(line)
		heappush(heap, (word, freq_map, index))

	return tup

def line_to_tuple(line):
	if line == '':
		return None, None
	line = line.rstrip("\n")
	tokens = line.split(" ")
	word = tokens[0]
	freq_map = tokens[1:]

	return word, freq_map

def tuple_to_values(tup):
	word = tup[0]
	freq_map = tup[1]
	index = tup[2]

	return word, freq_map, index

def get_next_word(tup):
	return tup[0]

def get_files_to_read(partial_index_dir):
	read_files = []
	for path, subdirs, files in os.walk(partial_index_dir):
		for name in files:
			file_name = os.path.join(path, name)
			read_files.append(open(file_name, 'r'))
	return read_files

def construct_heap(read_files):
	heap = []
	for i in range(len(read_files)):
		line = read_files[i].readline()
		word, freq_map = line_to_tuple(line)
		if word == None:
			continue
		heap.append((word, freq_map, i))
	heapify(heap)
	#print(heap)
	return heap

def sort_map(freq_map):
	ll = []
	for i in range(0, len(freq_map) - 1, 2):
		sublist = [int(freq_map[i]), int(freq_map[i + 1])]
		ll.append(sublist)
	ll.sort(key=lambda x:-x[1])  
	flatlist = [str(item) for sublist in ll for item in sublist]
	return flatlist

def merge_partial_indices(heap):
	write_files = [open(index_dir + "/" + x + ".txt", "a") for x in alphabets + digits]
	offset_files = [open(offset_dir + "/" + x + ".txt", "a") for x in alphabets + digits]
	while len(heap) > 0:
		smallest_word = pop_and_push_next(heap, read_files)
		word, freq_map, index = tuple_to_values(smallest_word)
		while len(heap) > 0 and word == get_next_word(heap[0]):
			next_word = pop_and_push_next(heap, read_files)
			_, next_word_freq_map, _ = tuple_to_values(next_word)
			freq_map += next_word_freq_map
		# sort (article_id, frequency) list in descending order of frequency
		freq_map = sort_map(freq_map)
		#print(word, *freq_map)
		freq_map = ' '.join(freq_map)
		line = word + " " + freq_map + "\n"
		first_char_word = word[0]
		if first_char_word.isalpha():
			write_index = ord(word[0]) - ord('a')
		else:
			write_index = len(alphabets) + int(first_char_word)
		offset_files[write_index].write("{0} {1}\n".format(str(write_files[write_index].tell()), word))
		write_files[write_index].write(line)

	for file in read_files:
		file.close()
	for file in write_files:
		file.close()
	for file in offset_files:
		file.close()

if __name__ == "__main__":

	if not os.path.exists(index_dir):
		os.makedirs(index_dir)
		os.makedirs(offset_dir)

	read_files = get_files_to_read(partial_index_dir)
	heap = construct_heap(read_files)
	merge_partial_indices(heap)
	print("--- %s seconds ---" % (time.time() - start_time))
	



