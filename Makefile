all: report.pdf

report.pdf: report.tex
	latex report.tex
	latex report.tex
	latex report.tex
	dvipdf report.dvi
	
clean:
	rm -f *.log *.blg *.bak *.aux *.pdf
	
