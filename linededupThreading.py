from multiprocessing.pool import ThreadPool
from itertools import islice

lines = set()

def addHashToLines(line):
	for l in line:
		lines.add(hash(l))

filepath = ''

pool = ThreadPool()
with open(filepath, 'r') as f:
	while True:
		linesChunk = list(islice(f, 100000))
		linesChunks = [linesChunk] + [list(islice(f, 100000)) for _ in range(7)]
		if linesChunk:
			pool.map(addHashToLines, linesChunks)
		else:
			break
		print(len(lines))

	f.seek(0)

	for line in f:
		if hash(line) in lines:
			print(line, end = "")
			lines.remove(hash(line))
