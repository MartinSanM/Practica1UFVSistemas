GCC=gcc
ARGS=-g

FileProcessing: FileProcessing.c
	${GCC} ${ARGS} $< -o $@


GENARGS=-l 6 -s 5 -u 4 -f 4 -t fulcrum

gen:
	./generate.sh ${GENARGS}

clean:
	rm -f FileProcessing

cl:
	rm -f Files/*.csv
