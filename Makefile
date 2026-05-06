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

.PHONY: copyright
copyright:
	grep -EL "Copyright \(c\) 20.* Aiven" $(shell git ls-files "*.py" | grep -v __init__.py)

.PHONY: coverage
coverage:
	[ -d "$(PYTEST_TMP)" ] || mkdir -p "$(PYTEST_TMP)"
	$(PYTHON) -m pytest $(PYTEST_ARG) --cov-report term-missing --cov-branch --cov-report xml:coverage.xml --cov myhoard test/


.PHONY: clean
clean:
	$(RM) -r *.egg-info/ build/ dist/
	$(RM) ../myhoard_* test-*.xml $(generated)

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


.PHONY: build-dep-ubuntu
build-dep-ubuntu:
	sudo apt install -y lsb-release wget tzdata libpq5 libpq-dev software-properties-common build-essential rsync curl git libaio1 libmecab2 psmisc python-is-python3

# local development, don't use in CI
# prerequisite
.PHONY: build-setup-specific-image
build-setup-specific-image:
ifeq ($(shell uname -m),x86_64)
	@echo "Building image for default architecture"
	PYTHON_VERSION=$(PYTHON_VERSION) MYSQL_VERSION=$(MYSQL_VERSION) PERCONA_VERSION=$(PERCONA_VERSION) \
		scripts/build-setup-specific-test-image
else
    # For other architectures, we must build the dependencies ourselves, as they are not available in the official repos.
	@echo "Building image for $(shell uname -m)"
	PYTHON_VERSION=$(PYTHON_VERSION) MYSQL_VERSION=$(MYSQL_VERSION) PERCONA_VERSION=$(PERCONA_VERSION) \
		scripts/build-setup-specific-test-image-full
endif

.PHONY: dockertest
dockertest:
	docker run --cap-add SYS_ADMIN -it --rm myhoard-test-temp /src/scripts/test-inside

# when the image didn't change this can be used. local dev only, don't use in CI
# in this target we override the /src that gets used to rsync source inside the container
.PHONY: dockertest-resync
dockertest-resync:
	docker run --cap-add SYS_ADMIN -it --rm -v "$(shell pwd):/src:ro" myhoard-test-temp /src/scripts/test-inside

.PHONY: dockertest-pytest
dockertest-pytest:
	docker run --cap-add SYS_ADMIN -it --rm -v "$(shell pwd):/src:ro" myhoard-test-temp /src/scripts/pytest-inside $(PYTEST_ARGS)
