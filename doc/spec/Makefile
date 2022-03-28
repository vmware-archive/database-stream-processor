TEXSOURCES=$(wildcard *.tex)

main.pdf: ${TEXSOURCES} main.bib
	pdflatex main
	bibtex main
	pdflatex main
	pdflatex main

clean:
	rm -f *.aux *.bbl *.log *.blg *.aux *.out
