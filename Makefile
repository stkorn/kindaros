COMPOSE := docker compose
IN_APP := $(COMPOSE) run --rm --no-deps -T stream

.PHONY: lock up doctor smoke check offline references down

lock:
	uv lock
	uv export --frozen --no-emit-project -o requirements.lock

up:
	$(COMPOSE) up -d --build --wait

doctor:
	$(COMPOSE) exec -T stream kinda doctor

smoke:
	uv run --frozen --only-group e2e pytest -q -p no:cacheprovider tests/e2e
	$(COMPOSE) exec -T stream pytest -q -p no:cacheprovider tests/integration

check: references offline
	$(IN_APP) mypy --cache-dir /tmp/mypy
	$(IN_APP) pytest -q -p no:cacheprovider tests/unit

offline:
	docker run --rm --network none kinda-m1-app:dev kinda offline-selftest

references:
	cmp docs/reference/001_initial.sql migrations/001_initial.sql
	cmp docs/reference/pos-event-v1.schema.json contracts/pos-event-v1.schema.json
	cmp docs/reference/sale.example.json contracts/sale.example.json
	cmp docs/reference/kafka.properties config/kafka.properties

down:
	$(COMPOSE) down
