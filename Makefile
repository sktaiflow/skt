book:
	jupyter-book build --all book/
	ghp-import -n -p -f book/_build/html
