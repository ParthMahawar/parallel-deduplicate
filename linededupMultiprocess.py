from multiprocessing import Pool
from multiprocessing.managers import BaseManager
from itertools import islice
from functools import partial
from hashlib import md5


class ParallelSet():
	def __init__(self):
		self.data = set()

	def add(self, val):
		self.data.add(val)

	def getSet(self):
		return self.data

	def contains(self, item):
		return item in self.data

	def remove(self, item):
		self.data.remove(item)

class CustomManager(BaseManager):
	...

def addHashToLines(lines, lineset):
	for line in lines:
		lineset.add(md5(line.encode()).hexdigest())

def printAndRemove(lines, lineset):
	for line in lines:
		if lineset.contains(md5(line.encode()).hexdigest()):
			print(line, end = "")
			lineset.remove(md5(line.encode()).hexdigest())

if __name__ == "__main__":
	filepath = ""
	
	CustomManager.register('ParallelSet', ParallelSet)
	manager = CustomManager()
	manager.start()
	
	lines = manager.ParallelSet()

	pool = Pool()

	with open(filepath, "r") as f:
		while True:
			linesChunk = list(islice(f, 10000))
			linesChunks = [linesChunk] + [list(islice(f, 10000)) for _ in range(7)]
			if linesChunk:
				pool.map(partial(addHashToLines, lineset = lines), linesChunks)
			else:
				break

		counter = 0

		f.seek(0)

		print(lines)
		theset = lines.getSet()

		## Single threaded deduplication
		for line in f:
			if md5(line.encode()).hexdigest() in theset:
				print(line, end = "")
				theset.remove(md5(line.encode()).hexdigest())

		## Parallel deduplication - doesn't account for race conditons, order of lines
		## might not remain, might error out
		
		'''while True:
			linesChunk = list(islice(f, 10000))
			linesChunks = [linesChunk] + [list(islice(f, 10000)) for _ in range(7)]
			if linesChunk:
				pool.map(partial(printAndRemove, lineset = lines), linesChunks)
			else:
				break'''