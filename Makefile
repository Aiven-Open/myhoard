short_ver = $(shell git describe --abbrev=0)
long_ver = $(shell git describe --long 2>/dev/null || echo $(short_ver)-0-unknown-g`git describe --always`)
generated = myhoard/version.py

all: $(generated)

PYTHON ?= python3
PYTHON_SOURCE_DIRS = myhoard/ test/

PYTEST_TMP ?= /var/tmp/pytest-of-$(USER)
PYTEST_ARG ?= -vvv --log-level=INFO --basetemp "$(PYTEST_TMP)"

MYSQL_SERVER_PACKAGE ?= mysql-server >= 8.0

.PHONY: unittest
unittest: version
	$(PYTHON) -m pytest -vv test/

.PHONY: typecheck
typecheck: version
	$(PYTHON) -m mypy --show-absolute-path $(PYTHON_SOURCE_DIRS)

.PHONY: lint
lint: version
	$(PYTHON) -m pylint --rcfile .pylintrc $(PYTHON_SOURCE_DIRS)

.PHONY: fmt
fmt: version
	unify --quote '"' --recursive --in-place $(PYTHON_SOURCE_DIRS)
	isort --recursive $(PYTHON_SOURCE_DIRS)
	yapf --parallel --recursive --in-place $(PYTHON_SOURCE_DIRS)

.PHONY: copyright
copyright:
	grep -EL "Copyright \(c\) 20.* Aiven" $(shell git ls-files "*.py" | grep -v __init__.py)

.PHONY: coverage
coverage: version
	$(PYTHON) -m pytest $(PYTEST_ARG) --cov-report term-missing --cov myhoard test/

.PHONY: clean
clean:
	$(RM) -r *.egg-info/ build/ dist/
	$(RM) ../myhoard_* test-*.xml $(generated)

myhoard/version.py: version.py
	$(PYTHON) $^ $@

.PHONY: version
version: myhoard/version.py

.PHONY: rpm
rpm: $(generated)
	git archive --output=myhoard-rpm-src.tar --prefix=myhoard/ HEAD
	# add generated files to the tar, they're not in git repository
	tar -r -f myhoard-rpm-src.tar --transform=s,myhoard/,myhoard/myhoard/, $(generated)
	rpmbuild -bb myhoard.spec \
		--define '_topdir $(PWD)/rpm' \
		--define '_sourcedir $(CURDIR)' \
		--define 'major_version $(short_ver)' \
		--define 'minor_version $(subst -,.,$(subst $(short_ver)-,,$(long_ver)))'
	$(RM) myhoard-rpm-src.tar

.PHONY: build-dep-fedora
build-dep-fedora:
	sudo dnf install -y 'dnf-command(builddep)'
	sudo dnf -y builddep myhoard.spec
	sudo dnf -y install --best --allowerasing --setopt=install_weak_deps=False \
		--exclude=mariadb-server "$(MYSQL_SERVER_PACKAGE)"

.PHONY: copyright
copyright:
	grep -EL "Copyright \(c\) 20.* Aiven" $(shell git ls-files "*.py" | grep -v __init__.py)

.PHONY: coverage
coverage: $(generated)
	$(PYTHON) -m coverage run --source myhoard -m pytest $(PYTEST_ARG) test/
	$(PYTHON) -m coverage report --show-missing
