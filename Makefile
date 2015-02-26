VENV := $(CURDIR)/venv
export PATH := $(VENV)/bin:$(PATH)

test: install
	paver test_all

install: $(VENV)
	$(VENV)/bin/pip install -r requirements-dev.txt

$(VENV):
	virtualenv $@

requirements.txt:
	pip freeze > $@
