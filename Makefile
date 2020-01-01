.PHONY: publish-on-pypi

publish-on-pypi:
	tox
	python setup.py sdist bdist_wheel
	python -m twine upload dist/* --skip-existing
