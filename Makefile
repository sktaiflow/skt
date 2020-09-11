all:
	sudo jupyter-book build -W -n book/
	ghp-import -n -p -f book/_build/html
