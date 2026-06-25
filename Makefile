# pitchflow — real-time football match-event streaming lakehouse

.PHONY: help up down logs topics \
        download replay \
        bronze silver gold peek \
        dashboard \
        test lint \
        clean reset

help:
	@echo ""
	@echo "  pitchflow — real-time football streaming lakehouse"
	@echo ""
	@echo "  Infrastructure"
	@echo "    make up          Start Redpanda + create topic (UI: localhost:8080)"
	@echo "    make down        Stop all containers"
	@echo "    make topics      Create the match.events Kafka topic"
	@echo "    make logs        Tail Redpanda logs"
	@echo ""
	@echo "  Data pipeline (each command in its own terminal, in order)"
	@echo "    make download    Cache the match from StatsBomb (once)"
	@echo "    make bronze      Kafka -> Bronze Delta  (keep running)"
	@echo "    make silver      Bronze -> Silver Delta (keep running)"
	@echo "    make gold        Silver -> Gold Delta   (keep running)"
	@echo "    make replay      Stream the match into Kafka"
	@echo ""
	@echo "  Serving"
	@echo "    make dashboard   Streamlit live dashboard on port 8501"
	@echo ""
	@echo "  Inspection"
	@echo "    make peek        Inspect Bronze Delta (pass path arg for other tables)"
	@echo ""
	@echo "  Quality"
	@echo "    make test        Run all tests (no infra needed)"
	@echo "    make lint        Run ruff linter"
	@echo ""
	@echo "  Housekeeping"
	@echo "    make clean       Remove cached event JSON files"
	@echo "    make reset       Full reset: stop Docker + wipe Delta + checkpoints"
	@echo ""

up:
	docker compose up -d
	@sleep 3
	@$(MAKE) topics
	@echo ""
	@echo "Redpanda console: http://localhost:8080"

down:
	docker compose down

topics:
	@docker exec pitchflow-redpanda rpk topic create match.events 2>/dev/null \
		&& echo "Topic match.events created" \
		|| echo "Topic match.events already exists"

logs:
	docker compose logs -f redpanda

download:
	python -m producer.download

replay:
	python -m producer.replay

bronze:
	python -m streaming.bronze

silver:
	python -m streaming.silver

gold:
	python -m streaming.gold

dashboard:
	streamlit run dashboard/app.py --server.port 8501

peek:
	python -m streaming.peek

test:
	python -m pytest producer/tests/ streaming/tests/ -v --tb=short

lint:
	ruff check producer/ streaming/ dashboard/

clean:
	rm -f data/events_*.json

reset: down
	rm -rf data/delta data/checkpoints spark-warehouse
	@echo "Reset complete. Run: make up && make bronze && make silver && make gold && make replay"
