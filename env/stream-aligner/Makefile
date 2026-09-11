COMPOSE ?= docker compose

.PHONY: up down logs demo test unit psql

up:            ## build and start the whole stack
	$(COMPOSE) up -d --build

down:          ## stop and remove containers + volumes
	$(COMPOSE) down -v

logs:          ## tail all service logs
	$(COMPOSE) logs -f --tail=100

demo:          ## run the scripted end-to-end walkthrough
	bash scripts/demo.sh

test:          ## e2e test against a running stack
	python3 tests/test_e2e.py
	python3 tests/test_orders_e2e.py
	python3 tests/test_perkey_e2e.py
	python3 tests/test_grace_e2e.py
	python3 tests/test_release_e2e.py
	python3 tests/test_backfill_e2e.py
	python3 tests/test_posting_e2e.py
	python3 tests/test_recon_e2e.py
	python3 tests/test_settlement_e2e.py
	python3 tests/test_carry_e2e.py

unit:          ## dependency-free unit tests of the alignment core
	python3 tests/test_core.py
	python3 tests/test_order_core.py
	python3 tests/test_recon_core.py
	python3 tests/test_settlement_core.py
	python3 tests/test_carry_core.py

psql:          ## open psql on the aligner database
	$(COMPOSE) exec postgres psql -U postgres -d aligner
