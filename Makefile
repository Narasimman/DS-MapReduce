# This is the Makefile helping you submit the labs.  

LABS=" lab1 lab2 lab3 lab4 lab5"

%:
	    tar cvzf $@-handin.tar.gz --exclude=src/main/kjv12.txt --exclude=Makefile --exclude=.git src; \
